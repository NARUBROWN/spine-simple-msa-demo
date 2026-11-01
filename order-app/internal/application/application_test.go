package application

import "testing"

func TestBuildApplicationKeepsConsumerAndPublisherPrefixAligned(t *testing.T) {
	t.Setenv("SPINE_DEMO_TOPIC_PREFIX", "test.")
	t.Setenv("SPINE_DEMO_KAFKA_BROKER", "127.0.0.1:1")
	app, options, err := BuildApplication()
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Validate(options); err != nil {
		t.Fatal(err)
	}
	registrations := app.Consumers().Registrations()
	if len(registrations) != 2 || registrations[0].Topic != "test.stock.reserved" || registrations[1].Topic != "test.stock.rejected" || options.Kafka.Write.TopicPrefix != "test." {
		t.Fatalf("consumer/publisher prefix mismatch: %#v / %#v", registrations, options.Kafka.Write)
	}
}
