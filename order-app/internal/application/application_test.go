package application

import (
	"github.com/NARUBROWN/spine"
	"testing"
)

func TestBuildApplicationRegistersLogicalSubscriptionsWithoutConnecting(t *testing.T) {
	t.Setenv("SPINE_DEMO_TOPIC_PREFIX", "test.")
	t.Setenv("SPINE_DEMO_KAFKA_BROKER", "127.0.0.1:1")
	app, options, err := BuildApplication()
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Validate(options); err != nil {
		t.Fatal(err)
	}
	description, err := spine.DescribeRegistrations(app)
	if err != nil {
		t.Fatal(err)
	}
	registrations := description.Consumers
	for topic, method := range map[string]string{"stock.reserved": "OnReserved", "stock.rejected": "OnRejected"} {
		found := false
		for _, registration := range registrations {
			if registration.Topic == topic && registration.Handler.Method == method {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("consumer %s.%s is missing: %#v", topic, method, registrations)
		}
	}
}
