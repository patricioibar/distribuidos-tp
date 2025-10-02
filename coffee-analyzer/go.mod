module cofee-analyzer

go 1.25

replace github.com/patricioibar/distribuidos-tp/communication => ../communication

require (
	github.com/op/go-logging v0.0.0-20160315200505-970db520ece7
	github.com/patricioibar/distribuidos-tp/innercommunication v0.0.0-20251002075226-e4eb542ee7af
	github.com/patricioibar/distribuidos-tp/middleware v0.0.0-20251002073917-38b6fc0804c5
	github.com/spf13/viper v1.21.0
)

require (
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/rabbitmq/amqp091-go v1.10.0 // indirect
	github.com/sagikazarmark/locafero v0.11.0 // indirect
	github.com/sourcegraph/conc v0.3.1-0.20240121214520-5f936abd7ae8 // indirect
	github.com/spf13/afero v1.15.0 // indirect
	github.com/spf13/cast v1.10.0 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.28.0 // indirect
)
