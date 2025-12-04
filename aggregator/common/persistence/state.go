package persistence

import (
	"encoding/json"
	"fmt"

	"github.com/patricioibar/distribuidos-tp/bitmap"
	pers "github.com/patricioibar/distribuidos-tp/persistance"
)

type PersistentState struct {
	AggregatedData    map[string][]interface{}
	AggregatedBatches *bitmap.Bitmap
	EmptyBatches      *bitmap.Bitmap

	// Reducer specific
	ReceivedAggregatorsBatches map[string]*bitmap.Bitmap
	AggregatorsDone            []string
	DuplicatedBatches          map[string]*bitmap.Bitmap
}

func NewPersistentState() *PersistentState {
	return &PersistentState{
		AggregatedData:             make(map[string][]interface{}),
		AggregatedBatches:          bitmap.New(),
		EmptyBatches:               bitmap.New(),
		ReceivedAggregatorsBatches: make(map[string]*bitmap.Bitmap),
		AggregatorsDone:            make([]string, 0),
		DuplicatedBatches:          make(map[string]*bitmap.Bitmap),
	}
}

func (s *PersistentState) Serialize() ([]byte, error) {
	type snapshot struct {
		AggregatedData             map[string][]interface{}      `json:"aggregated_data"`
		AggregatedBatches          *bitmap.JSONBitmap            `json:"aggregated_batches"`
		EmptyBatches               *bitmap.JSONBitmap            `json:"empty_batches"`
		ReceivedAggregatorsBatches map[string]*bitmap.JSONBitmap `json:"received_aggregators_batches"`
		DuplicatedBatches          map[string]*bitmap.JSONBitmap `json:"duplicated_batches"`
		AggregatorsDone            []string                      `json:"aggregators_done"`
	}

	snap := snapshot{
		AggregatedData:             s.AggregatedData,
		AggregatedBatches:          &bitmap.JSONBitmap{Bitmap: s.AggregatedBatches},
		EmptyBatches:               &bitmap.JSONBitmap{Bitmap: s.EmptyBatches},
		DuplicatedBatches:          make(map[string]*bitmap.JSONBitmap, len(s.DuplicatedBatches)),
		ReceivedAggregatorsBatches: make(map[string]*bitmap.JSONBitmap, len(s.ReceivedAggregatorsBatches)),
		AggregatorsDone:            s.AggregatorsDone,
	}
	for k, b := range s.ReceivedAggregatorsBatches {
		snap.ReceivedAggregatorsBatches[k] = &bitmap.JSONBitmap{Bitmap: b}
	}
	for k, b := range s.DuplicatedBatches {
		snap.DuplicatedBatches[k] = &bitmap.JSONBitmap{Bitmap: b}
	}

	return json.Marshal(snap)
}

func (s *PersistentState) Deserialize(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	type snapshot struct {
		AggregatedData             map[string][]interface{}      `json:"aggregated_data"`
		AggregatedBatches          *bitmap.JSONBitmap            `json:"aggregated_batches"`
		EmptyBatches               *bitmap.JSONBitmap            `json:"empty_batches"`
		ReceivedAggregatorsBatches map[string]*bitmap.JSONBitmap `json:"received_aggregators_batches"`
		DuplicatedBatches          map[string]*bitmap.JSONBitmap `json:"duplicated_batches"`
		AggregatorsDone            []string                      `json:"aggregators_done"`
	}

	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	// assign fields (user guarantees non-nil allocations, but be defensive)
	if snap.AggregatedData != nil {
		s.AggregatedData = snap.AggregatedData
	} else {
		s.AggregatedData = make(map[string][]interface{})
	}
	if snap.AggregatedBatches != nil && snap.AggregatedBatches.Bitmap != nil {
		s.AggregatedBatches = snap.AggregatedBatches.Bitmap
	} else {
		s.AggregatedBatches = bitmap.New()
	}
	if snap.EmptyBatches != nil && snap.EmptyBatches.Bitmap != nil {
		s.EmptyBatches = snap.EmptyBatches.Bitmap
	} else {
		s.EmptyBatches = bitmap.New()
	}
	if snap.ReceivedAggregatorsBatches != nil {
		s.ReceivedAggregatorsBatches = make(map[string]*bitmap.Bitmap, len(snap.ReceivedAggregatorsBatches))
		for k, jb := range snap.ReceivedAggregatorsBatches {
			if jb != nil {
				s.ReceivedAggregatorsBatches[k] = jb.Bitmap
			} else {
				s.ReceivedAggregatorsBatches[k] = bitmap.New()
			}
		}
	} else {
		s.ReceivedAggregatorsBatches = make(map[string]*bitmap.Bitmap)
	}
	if snap.DuplicatedBatches != nil {
		s.DuplicatedBatches = make(map[string]*bitmap.Bitmap)
		for k, jb := range snap.DuplicatedBatches {
			if jb != nil {
				s.DuplicatedBatches[k] = jb.Bitmap
			} else {
				s.DuplicatedBatches[k] = bitmap.New()
			}
		}
	} else {
		s.DuplicatedBatches = make(map[string]*bitmap.Bitmap)
	}
	if snap.AggregatorsDone != nil {
		s.AggregatorsDone = snap.AggregatorsDone
	} else {
		s.AggregatorsDone = make([]string, 0)
	}

	return nil
}

func (s *PersistentState) Apply(op pers.Operation) error {
	switch op.TypeID() {
	case AggregateBatchTypeID:
		opp, ok := op.(*AggregateBatchOp)
		if !ok {
			return fmt.Errorf("unexpected op type for typeID %d", AggregateBatchTypeID)
		}
		return s.aggregateBatchOp(opp)
	case AddEmptyBatchesTypeID:
		opp, ok := op.(*AddEmptyBatchesOp)
		if !ok {
			return fmt.Errorf("unexpected op type for typeID %d", AddEmptyBatchesTypeID)
		}
		return s.addEmptyBatchesOp(opp)
	case ReceiveAggregatorBatchTypeID:
		opp, ok := op.(*ReceiveAggregatorBatchOp)
		if !ok {
			return fmt.Errorf("unexpected op type for typeID %d", ReceiveAggregatorBatchTypeID)
		}
		return s.receiveAggregatorBatchOp(opp)
	case AddAggregatorDoneTypeID:
		opp, ok := op.(*AddAggregatorDoneOp)
		if !ok {
			return fmt.Errorf("unexpected op type for typeID %d", AddAggregatorDoneTypeID)
		}
		return s.addAggregatorDoneOp(opp)
	case RevertBatchAggregationTypeID:
		opp, ok := op.(*RevertBatchAggregationOp)
		if !ok {
			return fmt.Errorf("unexpected op type for typeID %d", RevertBatchAggregationTypeID)
		}
		return s.revertBatchAggregationOp(opp)
	default:
		return fmt.Errorf("unknown operation type ID: %d", op.TypeID())
	}
}

func (s *PersistentState) aggregateBatchOp(opp *AggregateBatchOp) error {
	s.AggregatedBatches.Add(uint64(opp.Seq))
	return s.aggregateData(opp.Data)
}

func (s *PersistentState) aggregateData(data map[string][]interface{}) error {
	for k, vArr := range data {
		_, ok := s.AggregatedData[k]
		if !ok {
			s.AggregatedData[k] = vArr
		} else {
			for i, v := range vArr {
				for i >= len(s.AggregatedData[k]) {
					s.AggregatedData[k] = append(s.AggregatedData[k], float64(0))
				}

				switch existing := s.AggregatedData[k][i].(type) {
				case float64:
					if newVal, ok := v.(float64); ok {
						s.AggregatedData[k][i] = existing + newVal
					} else {
						return fmt.Errorf("type mismatch when merging aggregated data for key %s index %d: %T", k, i, v)
					}
				case int:
					if newVal, ok := v.(int); ok {
						s.AggregatedData[k][i] = existing + newVal
					} else {
						return fmt.Errorf("type mismatch when merging aggregated data for key %s index %d: %T", k, i, v)
					}
				default:
					return fmt.Errorf("unsupported data type for aggregation for key %s index %d: %T", k, i, v)
				}
			}
		}
	}
	return nil
}

func (s *PersistentState) addEmptyBatchesOp(opp *AddEmptyBatchesOp) error {
	s.EmptyBatches.Or(opp.EmptyBatchesSeqs)
	return nil
}

func (s *PersistentState) ReceivedAggregatorBatchesFor(aggregatorID string) *bitmap.Bitmap {
	bm, ok := s.ReceivedAggregatorsBatches[aggregatorID]
	if !ok {
		bm = bitmap.New()
		s.ReceivedAggregatorsBatches[aggregatorID] = bm
	}
	return bm
}

func (s *PersistentState) DuplicatedBatchesFor(aggregatorID string) *bitmap.Bitmap {
	bm, ok := s.DuplicatedBatches[aggregatorID]
	if !ok {
		bm = bitmap.New()
		s.DuplicatedBatches[aggregatorID] = bm
	}
	return bm
}

func (s *PersistentState) receiveAggregatorBatchOp(opp *ReceiveAggregatorBatchOp) error {
	bm := s.ReceivedAggregatorBatchesFor(opp.AggregatorID)
	bm.Add(uint64(opp.Seq))
	return s.aggregateData(opp.Data)
}

func (s *PersistentState) addAggregatorDoneOp(opp *AddAggregatorDoneOp) error {
	duplicatedBatches := bitmap.And(s.AggregatedBatches, opp.Seqs)
	s.AggregatedBatches.Or(opp.Seqs)
	s.DuplicatedBatches[opp.AggregatorDone] = duplicatedBatches
	s.AggregatorsDone = append(s.AggregatorsDone, opp.AggregatorDone)
	return nil
}

func (s *PersistentState) revertBatchAggregationOp(opp *RevertBatchAggregationOp) error {
	dupBatches := s.DuplicatedBatchesFor(opp.Aggregator)
	dupBatches.RemoveRange(opp.Seq, opp.Seq+1)
	neg := make(map[string][]interface{}, len(opp.Data))
	for k, arr := range opp.Data {
		negArr := make([]interface{}, len(arr))
		for i, v := range arr {
			switch val := v.(type) {
			case float64:
				negArr[i] = -val
			case int:
				negArr[i] = -val
			default:
				return fmt.Errorf("unsupported data type for reversion for key %s index %d: %T", k, i, v)
			}
		}
		neg[k] = negArr
	}
	return s.aggregateData(neg)
}
