package id_test

import (
	"testing"

	id "github.com/da123wda/snowflake-id"
	mutexid "github.com/da123wda/snowflake-id/mutex"
)

func TestActorAndMutexPackagesAreIndependentlyUsable(t *testing.T) {
	actorGenerator, err := id.NewActor(10)
	if err != nil {
		t.Fatal(err)
	}
	mutexGenerator, err := mutexid.NewMutex(11)
	if err != nil {
		t.Fatal(err)
	}

	actorID, err := actorGenerator.Next()
	if err != nil {
		t.Fatal(err)
	}
	mutexID, err := mutexGenerator.Next()
	if err != nil {
		t.Fatal(err)
	}
	if actorID == mutexID {
		t.Fatalf("independent packages returned the same ID: %d", actorID)
	}

	actorParsed, err := id.Parse(actorID)
	if err != nil || actorParsed.V1 != 10 {
		t.Fatalf("actor Parse machine ID = %d, error %v", actorParsed.V1, err)
	}
	mutexParsed, err := mutexid.Parse(mutexID)
	if err != nil || mutexParsed.V1 != 11 {
		t.Fatalf("mutex Parse machine ID = %d, error %v", mutexParsed.V1, err)
	}
}
