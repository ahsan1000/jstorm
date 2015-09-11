package backtype.storm.messaging;

import java.nio.ByteBuffer;

public class TaskMessage {
	private int _task;
	private byte[] _message;
    private String componentId;
    private String stream;

	public TaskMessage(int task, byte[] message) {
		_task = task;
		_message = message;
	}

    public TaskMessage(int _task, byte[] _message, String componentId, String stream) {
        this._task = _task;
        this._message = _message;
        this.componentId = componentId;
        this.stream = stream;
    }

    public int task() {
		return _task;
	}

	public byte[] message() {
		return _message;
	}

    public String componentId() {
        return componentId;
    }

    public String stream() {
        return stream;
    }

    public static boolean isEmpty(TaskMessage message) {
		if (message == null) {
			return true;
		}else if (message.message() == null) {
			return true;
		}else if (message.message().length == 0) {
			return true;
		}
		
		return false;
	}

	@Deprecated
	public ByteBuffer serialize() {
		ByteBuffer bb = ByteBuffer.allocate(_message.length + 2);
		bb.putShort((short) _task);
		bb.put(_message);
		return bb;
	}

	@Deprecated
	public void deserialize(ByteBuffer packet) {
		if (packet == null)
			return;
		_task = packet.getShort();
		_message = new byte[packet.limit() - 2];
		packet.get(_message);
	}

}
