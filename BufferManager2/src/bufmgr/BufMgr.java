package bufmgr;

import global.GlobalConst;
import global.PageId;
import global.SystemDefs;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;

import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidRunSizeException;
import diskmgr.OutOfSpaceException;
import diskmgr.Page;

public class BufMgr {
	private static final Exception InvalidPageNumberException = null;
	private static final Exception NullPointerException = null;
	public int numbuffers;
	public byte[][] bufPool;
	public bufDescr[] descr;
	public HashMap<Integer, Integer> map;
	public HashMap<Integer, Integer> inv;
	public String replArg;
	public HashSet<Integer> wasInThePool;
	// public Stack<Integer> candidates;
	public Queue<Integer> candidates;

	public BufMgr(int PoolSize, String policy) {
		// TODO Auto-generated constructor stub
		numbuffers = PoolSize;
		replArg = policy;
		map = new HashMap<>();
		inv = new HashMap<>();
		wasInThePool = new HashSet<Integer>();
		// candidates = new Stack<Integer>();

		bufPool = new byte[numbuffers][GlobalConst.MAX_SPACE];
		descr = new bufDescr[numbuffers];
		for (int i = 0; i < PoolSize; i++) {
			descr[i] = new bufDescr(0, false, false);
		}

		if (replArg.equals("FIFO")) {

			candidates = new LinkedList<Integer>();

		} else if (replArg.equals("LRU")) {

			candidates = new PriorityQueue<>(numbuffers,
					new Comparator<Integer>(

					) {

						@Override
						public int compare(Integer o1, Integer o2) {
							// TODO Auto-generated method stub
							int frameNumberOne = map.get(o1);
							int frameNumberTwo = map.get(o2);
							return descr[frameNumberOne].totalPinCount
									- descr[frameNumberTwo].totalPinCount;

						}
					});

		} else if (replArg.equals("MRU")) {
			candidates = new PriorityQueue<>(numbuffers,
					new Comparator<Integer>(

					) {

						@Override
						public int compare(Integer o1, Integer o2) {
							// TODO Auto-generated method stub
							int frameNumberOne = map.get(o1);
							int frameNumberTwo = map.get(o2);
							return descr[frameNumberTwo].totalPinCount
									- descr[frameNumberOne].totalPinCount;

						}
					});

		} else {
			candidates = new PriorityQueue<>(numbuffers,
					new Comparator<Integer>(

					) {

						@Override
						public int compare(Integer o1, Integer o2) {
							// TODO Auto-generated method stub
							int frameNumberOne = map.get(o1);
							int frameNumberTwo = map.get(o2);

							int numberOne = descr[frameNumberOne].loved ? 1 : 0;
							int numberTwo = descr[frameNumberTwo].loved ? 1 : 0;

							return numberOne - numberTwo;

						}
					});

		}

	}

	public int getVacantFrame() {
		for (int i = 0; i < numbuffers; i++) {
			if (!map.containsValue(i))
				return i;
		}
		return -1;
	}

	public void pinPage(PageId id, Page page, boolean empty, boolean loved)
			throws Exception {
		if (map.containsKey(id.pid)) {
			int frame = map.get(id.pid);
			descr[frame].loved = loved;
			descr[frame].pinCount = descr[frame].pinCount + 1;
			descr[frame].totalPinCount++;
			page.setpage(bufPool[frame]);
		} else {
			int frame = getVacantFrame();
			if (frame > -1) {
				Page tempPage = new Page();
				try {
					SystemDefs.JavabaseDB.read_page(id, tempPage);
				} catch (diskmgr.InvalidPageNumberException | FileIOException
						| IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				bufPool[frame] = tempPage.getpage();
				page.setpage(bufPool[frame]);
				descr[frame].loved = loved;
				descr[frame].pinCount = 1;
				descr[frame].totalPinCount = 1;
				map.put(id.pid, frame);
				inv.put(frame, id.pid);
				wasInThePool.add(id.pid);
			} else {
				// System.out.println(candidates.size());
				int temp_id = -1;
				if (candidates.size() > 0) {
					temp_id = candidates.poll();
					candidates.remove((Object) temp_id);
				}
				if (temp_id < 0)
					throw new BufferPoolExceededException(null,
							"bufmgr.BufferPoolExceededException");
				int temp_frame = map.get(temp_id);
				if (descr[temp_frame].dirtyBit)
					flushPage(new PageId(temp_id));
				Page tempPage = new Page();
				try {
					SystemDefs.JavabaseDB.read_page(id, tempPage);
				} catch (diskmgr.InvalidPageNumberException | FileIOException
						| IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				bufPool[temp_frame] = tempPage.getpage();
				page.setpage(bufPool[temp_frame]);
				descr[temp_frame].pinCount = 1;
				descr[temp_frame].totalPinCount = 1;
				descr[temp_frame].loved = loved;
				descr[temp_frame].dirtyBit = false;
				map.remove(new Integer(temp_id));
				inv.remove(new Integer(temp_frame));
				map.put(id.pid, temp_frame);
				inv.put(temp_frame, id.pid);
				wasInThePool.add(id.pid);

			}
		}
	}

	public void unpinPage(PageId id, boolean dirty, boolean loved)
			throws Exception {
		if (map.containsKey(id.pid)) {
			int frame = map.get(id.pid);
			if (descr[frame].pinCount == 0)
				throw InvalidPageNumberException;
			descr[frame].pinCount = descr[frame].pinCount - 1;
			descr[frame].dirtyBit = dirty;
			descr[frame].loved = loved;
			if (descr[frame].pinCount == 0) {
				if (!candidates.contains(id.pid)) {
					candidates.offer(id.pid);
				}
			}
		} else {
			throw new HashEntryNotFoundException(null,
					"bufmgr.HashEntryNotFoundException");
		}
	}

	public int getNumUnpinnedBuffers() {
		int temp = 0;
		for (int i = 0; i < descr.length; i++) {
			if (descr[i].pinCount == 0)
				temp++;
		}
		return temp;
	}

	public PageId newPage(Page first, int howmany) {
		PageId id1, id2;
		id1 = new PageId();
		id2 = new PageId();
		id1 = newPage(id1);
		try {
			pinPage(id1, first, false, false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int i = id1.pid + 1; i < (id1.pid) + howmany; i++) {
			id2 = newPage(id2);
			if (id2.pid == -1)
				return id1;
		}
		return id1;
	}

	private PageId newPage(PageId pid) {
		int checking = getVacantFrame();
		if (checking == -1) {
			if (candidates.size() != 0) {
				try {
					SystemDefs.JavabaseDB.allocate_page(pid);
				} catch (OutOfSpaceException | InvalidRunSizeException
						| diskmgr.InvalidPageNumberException | FileIOException
						| DiskMgrException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else
				return new PageId(-1);
		} else
			try {
				SystemDefs.JavabaseDB.allocate_page(pid);
			} catch (OutOfSpaceException | InvalidRunSizeException
					| diskmgr.InvalidPageNumberException | FileIOException
					| DiskMgrException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return pid;
	}

	public void flushPage(PageId pid) {
		try {
			SystemDefs.JavabaseDB.write_page(pid,
					new Page(bufPool[map.get(pid.pid)]));
		} catch (diskmgr.InvalidPageNumberException | FileIOException
				| IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void freePage(PageId pid) throws Exception {
		try {
			if (map.containsKey(pid.pid)) {
				if (descr[map.get(pid.pid)].pinCount <= 1) {
					if (descr[map.get(pid.pid)].pinCount == 1)
						unpinPage(pid, false, false);
					SystemDefs.JavabaseDB.deallocate_page(pid);
					candidates.remove((Object) pid.pid);
					map.remove((Object) pid.pid);
					inv.remove(map.get(pid.pid));
				} else {
					throw new PagePinnedException(null,
							"bufmgr.PagePinnedException");
				}
			} else if (!wasInThePool.contains(pid.pid)) {
				SystemDefs.JavabaseDB.deallocate_page(pid);
				map.remove((Object) (pid.pid));
				inv.remove((Object) pid.pid);
			} else {
				SystemDefs.JavabaseDB.deallocate_page(pid);
				map.remove((pid.pid));
				inv.remove((Object) pid.pid);
				wasInThePool.remove(new Integer(pid.pid));
			}
		} catch (InvalidRunSizeException | diskmgr.InvalidPageNumberException
				| FileIOException | DiskMgrException | IOException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}

	public void flushAllPages() {
		for (int i = 0; i < descr.length; i++) {
			if (inv.containsKey(i)) {
				PageId pid = new PageId(inv.get(i));
				flushPage(pid);
			}
		}
	}

}