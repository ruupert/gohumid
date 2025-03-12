package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-pg/pg/v10"
	"gopkg.in/yaml.v2"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	config_file       = flag.String("config", "config.yaml", "Config yaml path")
	lifetime          = flag.Duration("lifetime", 5*time.Second, "lifetime of process before shutdown (0s=infinite)")
	ErrLog            = log.New(os.Stderr, "[ERROR] ", log.LstdFlags|log.Lmsgprefix)
	Log               = log.New(os.Stdout, "[INFO] ", log.LstdFlags|log.Lmsgprefix)
	deliveryCount int = 0
	conf          GoHumidConfig
	db            *pg.DB
)

type GoHumidConfig struct {
	Config struct {
		Verbose bool `yaml:"verbose"`
		Db      struct {
			Name string `yaml:"name"`
			Host string `yaml:"host"`
			User string `yaml:"user"`
			Pass string `yaml:"pass"`
		} `yaml:"db"`
		Amqp struct {
			URI          string `yaml:"uri"`
			ExchangeName string `yaml:"exchange_name"`
			ExcahngeType string `yaml:"excahnge_type"`
			Queue        string `yaml:"queue"`
			Key          string `yaml:"key"`
			ConsumerTag  string `yaml:"consumer_tag"`
		} `yaml:"amqp"`
	} `yaml:"config"`
}

func (c *GoHumidConfig) getConf(conf string) *GoHumidConfig {
	flag.Parse()
	yamlFile, err := os.ReadFile(conf)
	if err != nil {
		ErrLog.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		ErrLog.Fatalf("Unmarshal: %v", err)
	}
	return c
}

func logger(l *log.Logger, s string) {
	if conf.Config.Verbose {
		l.Println(s)
	}
}

type ShellyHumidity struct {
	Time     time.Time
	Name     string
	Humidity float32
}

type ShellyTemp struct {
	Time time.Time
	Name string
	Temp float32
}

const createDbSQL = `CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE TABLE IF NOT EXISTS shelly_temps ( 
        time timestamptz not null,
        name TEXT,
        temp float
);
SELECT create_hypertable('shelly_temps', by_range('time'), if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS idx_name_time_temp ON shelly_temps(name, time);
CREATE TABLE IF NOT EXISTS shelly_humidities ( 
        time timestamptz not null,
        name TEXT,
        humidity float
);
SELECT create_hypertable('shelly_humidities', by_range('time'), if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS idx_name_time_hum ON shelly_humidities(name, time);
`

func init() {
	flag.Parse()
}

func main() {
	conf.getConf(*config_file)

	db = pg.Connect(&pg.Options{
		Database: conf.Config.Db.Name,
		Addr:     conf.Config.Db.Host,
		User:     conf.Config.Db.User,
		Password: conf.Config.Db.Pass,
		TLSConfig: &tls.Config{
			InsecureSkipVerify:       true,
			PreferServerCipherSuites: true,
		},
	})
	defer db.Close()

	db.Exec(createDbSQL)

	c, err := NewConsumer(conf.Config.Amqp.URI,
		conf.Config.Amqp.ExchangeName,
		conf.Config.Amqp.ExcahngeType,
		conf.Config.Amqp.Queue,
		conf.Config.Amqp.Key,
		conf.Config.Amqp.ConsumerTag,
	)
	if err != nil {
		fmt.Printf("%s", err)
	}

	SetupCloseHandler(c)

	if *lifetime > 0 {
		if conf.Config.Verbose {
			logger(Log, fmt.Sprintf("running for %s", *lifetime))
		}
		time.Sleep(*lifetime)
	} else {
		if conf.Config.Verbose {
			logger(Log, "running until Consumer is done")
		}
		<-c.done
	}
	if conf.Config.Verbose {
		logger(Log, "shutting down")
	}
	if err := c.Shutdown(); err != nil {
		ErrLog.Printf("error during shutdown: %s", err)
	}
	time.Sleep(5000)

}

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

func SetupCloseHandler(consumer *Consumer) {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		logger(Log, "Ctrl+C pressed in Terminal")
		if err := consumer.Shutdown(); err != nil {
			ErrLog.Fatalf("error during shutdown: %s", err)
		}
		os.Exit(0)
	}()
}

func NewConsumer(amqpURI, exchange, exchangeType, queueName, key, ctag string) (*Consumer, error) {
	c := &Consumer{
		conn:    nil,
		channel: nil,
		tag:     ctag,
		done:    make(chan error),
	}

	var err error

	config := amqp.Config{Properties: amqp.NewConnectionProperties()}
	config.Properties.SetClientConnectionName(conf.Config.Amqp.ConsumerTag)
	c.conn, err = amqp.DialConfig(amqpURI, config)
	if err != nil {
		return nil, fmt.Errorf("dial: %s", err)
	}

	go func() {
		logger(Log, fmt.Sprintf("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error))))
	}()

	logger(Log, "got connection, getting channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("channel: %s", err)
	}

	logger(Log, fmt.Sprintf("declared Exchange, declaring Queue %q", queueName))
	queue, err := c.channel.QueueDeclare(
		queueName, // name of the queue
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("queue declare: %s", err)
	}

	logger(Log, fmt.Sprintf("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
		queue.Name, queue.Messages, queue.Consumers, key))

	if err = c.channel.QueueBind(
		queue.Name, // name of the queue
		key,        // bindingKey
		exchange,   // sourceExchange
		false,      // noWait
		nil,        // arguments
	); err != nil {
		return nil, fmt.Errorf("queue bind: %s", err)
	}

	logger(Log, fmt.Sprintf("Queue bound to Exchange, starting Consume (consumer tag %q)", c.tag))
	deliveries, err := c.channel.Consume(
		queue.Name, // name
		c.tag,      // consumerTag,
		false,      // autoAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("queue consume: %s", err)
	}

	go handle(deliveries, c.done)

	return c, nil
}

func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer logger(Log, "AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.done
}

func handle(deliveries <-chan amqp.Delivery, done chan error) {

	cleanup := func() {
		logger(Log, "handle: deliveries channel closed")
		done <- nil
	}

	defer cleanup()

	for d := range deliveries {
		deliveryCount++
		logger(Log, fmt.Sprintf("delivery count %d", deliveryCount))
		if conf.Config.Verbose {
			logger(Log, fmt.Sprintf(
				"got %dB delivery: [%v] %q",
				len(d.Body),
				d.Acknowledger,
				d.Body,
			))
		}
		var floatValue float64
		str := string(d.Body)
		// Convert string to float64
		floatValue, err := strconv.ParseFloat(str, 64)
		if err != nil {
			fmt.Println("Error converting string to float:", err)
			return
		}
		if strings.Split(d.RoutingKey, ".")[3] == "humidity" {
			reading := &ShellyHumidity{
				Time:     d.Timestamp,
				Name:     strings.Split(d.RoutingKey, ".")[1],
				Humidity: float32(floatValue),
			}
			_, err := db.Model(reading).Insert()
			if err != nil {
				fmt.Println(err)
			} else {
				err := d.Ack(true)
				if err != nil {
					fmt.Println(err)
				}
			}
		}
		if strings.Split(d.RoutingKey, ".")[3] == "temperature" {
			reading := &ShellyTemp{
				Time: d.Timestamp,
				Name: strings.Split(d.RoutingKey, ".")[1],
				Temp: float32(floatValue),
			}
			_, err := db.Model(reading).Insert()
			if err != nil {
				fmt.Println(err)
			} else {
				err := d.Ack(true)
				if err != nil {
					fmt.Println(err)
				}
			}
		}
	}
}
