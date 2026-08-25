package actor_test

import (
	"testing"
	"time"

	actorid "github.com/da123wda/snowflake-id/actor"
	mutexid "github.com/da123wda/snowflake-id/mutex"
)

func TestActorAndMutexPackagesAreIndependentlyUsable(t *testing.T) {
	epoch := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	actorGenerator, err := actorid.NewActor(10, epoch)
	if err != nil {
		t.Fatal(err)
	}
	mutexGenerator, err := mutexid.NewMutex(11, epoch)
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

	actorParsed, err := actorid.Parse(actorID, epoch)
	if err != nil || actorParsed.V1 != 10 {
		t.Fatalf("actor Parse machine ID = %d, error %v", actorParsed.V1, err)
	}
	mutexParsed, err := mutexid.Parse(mutexID, epoch)
	if err != nil || mutexParsed.V1 != 11 {
		t.Fatalf("mutex Parse machine ID = %d, error %v", mutexParsed.V1, err)
	}
}
