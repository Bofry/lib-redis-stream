package redis

import "testing"

func TestCyclicCounter_WithCount_0(t *testing.T) {
	counter := newCyclicCounter(0)

	expectedAndGot := [][]bool{
		// run spin()  , expected
		{counter.spin(), false}, //  1st
		{counter.spin(), false}, //  2nd
		{counter.spin(), false}, //  3rd
		{counter.spin(), false}, //  4th
		{counter.spin(), false}, //  5th
		{counter.spin(), false}, //  6th
		{counter.spin(), false}, //  7th
		{counter.spin(), false}, //  8th
		{counter.spin(), false}, //  9th
		{counter.spin(), false}, // 10th
		{counter.spin(), false}, // 11th
		{counter.spin(), false}, // 12th
	}

	for i, v := range expectedAndGot {
		expected, got := v[1], v[0]
		if expected != got {
			t.Errorf("assert spin() at %d :: expected %+v, got %+v", (i + 1), expected, got)
		}
	}
}

func TestCyclicCounter_WithCount_1(t *testing.T) {
	counter := newCyclicCounter(1)

	expectedAndGot := [][]bool{
		// run spin()  , expected
		{counter.spin(), true}, //  1st
		{counter.spin(), true}, //  2nd
		{counter.spin(), true}, //  3rd
		{counter.spin(), true}, //  4th
		{counter.spin(), true}, //  5th
		{counter.spin(), true}, //  6th
		{counter.spin(), true}, //  7th
		{counter.spin(), true}, //  8th
		{counter.spin(), true}, //  9th
		{counter.spin(), true}, // 10th
		{counter.spin(), true}, // 11th
		{counter.spin(), true}, // 12th
	}

	for i, v := range expectedAndGot {
		expected, got := v[1], v[0]
		if expected != got {
			t.Errorf("assert spin() at %d :: expected %+v, got %+v", (i + 1), expected, got)
		}
	}
}

func TestCyclicCounter_WithCount_2(t *testing.T) {
	counter := newCyclicCounter(2)

	expectedAndGot := [][]bool{
		// run spin()  , expected
		{counter.spin(), false}, //  1st
		{counter.spin(), true},  //  2nd
		{counter.spin(), false}, //  3rd
		{counter.spin(), true},  //  4th
		{counter.spin(), false}, //  5th
		{counter.spin(), true},  //  6th
		{counter.spin(), false}, //  7th
		{counter.spin(), true},  //  8th
		{counter.spin(), false}, //  9th
		{counter.spin(), true},  // 10th
		{counter.spin(), false}, // 11th
		{counter.spin(), true},  // 12th
	}

	for i, v := range expectedAndGot {
		expected, got := v[1], v[0]
		if expected != got {
			t.Errorf("assert spin() at %d :: expected %+v, got %+v", (i + 1), expected, got)
		}
	}
}

func TestCyclicCounter_WithCount_3(t *testing.T) {
	counter := newCyclicCounter(3)

	expectedAndGot := [][]bool{
		// run spin()  , expected
		{counter.spin(), false}, //  1st
		{counter.spin(), false}, //  2nd
		{counter.spin(), true},  //  3rd
		{counter.spin(), false}, //  4th
		{counter.spin(), false}, //  5th
		{counter.spin(), true},  //  6th
		{counter.spin(), false}, //  7th
		{counter.spin(), false}, //  8th
		{counter.spin(), true},  //  9th
		{counter.spin(), false}, // 10th
		{counter.spin(), false}, // 11th
		{counter.spin(), true},  // 12th
	}

	for i, v := range expectedAndGot {
		expected, got := v[1], v[0]
		if expected != got {
			t.Errorf("assert spin() at %d :: expected %+v, got %+v", (i + 1), expected, got)
		}
	}
}
