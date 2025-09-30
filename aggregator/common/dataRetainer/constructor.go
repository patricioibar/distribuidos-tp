package dataretainer

func NewDataRetainer(retainings []Retaining) DataRetainer {
	if len(retainings) == 0 || retainings == nil {
		return &AllRetainer{}
	}
	return &TopRetainer{
		retainings: retainings,
	}
}
