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
	if options.Kafka.Write.TopicPrefix != "test." {
		t.Fatalf("consumer/publisher prefix mismatch: %#v / %#v", registrations, options.Kafka.Write)
	}
	for topic, method := range map[string]string{"test.stock.reserved": "OnReserved", "test.stock.rejected": "OnRejected"} {
		found := false
		for _, registration := range registrations {
			if registration.Topic == topic && registration.Meta.Method.Name == method {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("consumer %s.%s is missing: %#v", topic, method, registrations)
		}
	}
}
