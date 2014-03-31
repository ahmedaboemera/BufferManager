package bufmgr;

public class bufDescr {

	public int pinCount;
	public boolean dirtyBit;
	public boolean loved;
	public int totalPinCount;

	public bufDescr(int _pinCount, boolean _dirtyBit, boolean _loved) {
		pinCount = _pinCount;
		dirtyBit = _dirtyBit;
		loved = _loved;
		totalPinCount = 0;

	}

	public bufDescr() {
		pinCount = 0;
		dirtyBit = false;
		loved = false;
		totalPinCount = 0;

	}

}
