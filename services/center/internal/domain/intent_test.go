package domain

import "testing"

// Intent states are stored as their names, so every state must survive the round trip.
func TestIntentStateRoundTripsThroughItsName(t *testing.T) {
	all := []IntentState{IntentUnknown, IntentWaiting, IntentUploaded, IntentHashing, IntentHashed, IntentExpired, IntentFailed}
	if len(all) != len(stateName) {
		t.Fatalf("this test lists %d states but stateName has %d; add the new state here", len(all), len(stateName))
	}
	for _, s := range all {
		if got := StateFromString(s.String()); got != s {
			t.Errorf("StateFromString(%q) = %v; want %v", s.String(), got, s)
		}
	}
}

func TestUnknownStatusNamesMapToIntentUnknown(t *testing.T) {
	for _, name := range []string{"", "waiting", "BOGUS", "INDEXED"} {
		if got := StateFromString(name); got != IntentUnknown {
			t.Errorf("StateFromString(%q) = %v; want IntentUnknown: a status outside the state machine must not pass as a real state", name, got)
		}
	}
}
