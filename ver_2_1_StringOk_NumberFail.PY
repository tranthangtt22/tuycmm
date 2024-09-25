import time
import can
from threading import Event, Thread

# ------------------- CANTP ------------------- #
class CANTP(can.Listener):
    def __init__(self, bus, txid, rxid):
        self.bus = bus
        self.txid = txid
        self.rxid = rxid
        self.st_min_for_tx = 0x14  # 20ms
        self.blk_size_for_rx = 3    # Block size
        self.flow_ctrl_ok = Event()
        self.seq = 0
        self.received_blocks = 0
        self.data_complete = False  # Cờ để xác định quá trình nhận đã hoàn thành

    # Send message over CAN bus
    def sendMessage(self, msg):
        message = can.Message(arbitration_id=self.txid, data=msg, is_extended_id=False)
        self.bus.send(message)

    # Write Single Frame (SF)
    def writeSingleFrame(self, data):
        data_len = len(data)
        msg = [data_len] + data + [0x00] * (8 - len(data) - 1)  # Pad to 8 bytes
        print(f"Sending Single Frame: {msg}")
        self.sendMessage(msg)

    # Write First Frame (FF)
    def writeFirstFrame(self, data):
        data_len = len(data)
        msg = [0x10 | ((data_len & 0xF00) >> 8), data_len & 0xFF] + data[:6] # Dữ liệu trong First Frame là 6 byte
        print(f"Sending First Frame: {msg}")
        self.sendMessage(msg)
        return data[6:]

    # Write Consecutive Frame (CF)
    def writeConsecutiveFrame(self, data):
        self.seq = (self.seq + 1) % 16
        frame_data = data[:7]  # Dữ liệu trong Consecutive Frame là 7 byte
        msg = [0x20 | self.seq] + frame_data
        msg += [0x00] * (8 - len(msg))  # Thêm padding nếu cần để đủ 8 byte
        print(f"Sending Consecutive Frame: {msg}")
        self.sendMessage(msg)
        return data[7:]

    # Write Flow Control (FC)
    def writeFlowControlFrame(self):
        if self.data_complete:  # Ngừng gửi nếu đã nhận đủ dữ liệu
            return
        msg = [0x30, self.blk_size_for_rx, self.st_min_for_tx, 0x55, 0x55, 0x55, 0x55, 0x55]
        print(f"Sending Flow Control Frame (FC): {msg}")
        self.sendMessage(msg)

    def writeMultiFrame(self, data):
        # Reset the sequence and block count
        self.flow_ctrl_ok.clear()
        data = self.writeFirstFrame(data)
        data_len = len(data)
        block_count = 0

        while data_len:
            # Chờ nhận Flow Control Frame từ bên nhận, tối đa 1 giây
            if not self.flow_ctrl_ok.wait(1):  # Chờ 1 giây
                print("Flow Control timeout")
                break

            # Gửi block với số lượng frame bằng `blk_size_for_rx`
            for _ in range(self.blk_size_for_rx):
                if not data_len:
                    break
                data = self.writeConsecutiveFrame(data)
                data_len = len(data)
                block_count += 1
                time.sleep(self.st_min_for_tx / 1000)  # Dừng giữa các frame

            # Sau khi gửi xong một block, chờ Flow Control Frame mới
            self.flow_ctrl_ok.clear()

    # API for sending data
    def sendData(self, data):
        if isinstance(data, str):
            data = list(data.encode('utf-8'))  # Chuyển chuỗi thành mảng byte

        if len(data) <= 7:
            self.writeSingleFrame(data)
        else:
            th = Thread(target=self.writeMultiFrame, args=(data,))
            th.start()
            th.join()  # Trong môi trường thực, bạn có thể bỏ qua join để không làm tắc nghẽn luồng

    # Receive message
    def on_message_received(self, msg):
        can_id = msg.arbitration_id
        data = list(msg.data)  # Chuyển bytearray thành list để hiển thị tương tự như Sending

        if can_id == self.rxid:
            # Nếu đã hoàn thành việc nhận, ngừng xử lý bất kỳ frame nào nữa
            if self.data_complete:
                return

            # Xử lý Single Frame (Dữ liệu nhỏ hơn 8 byte)
            if data[0] & 0xF0 == 0x00:
                print(f"Received Single Frame: {data}")
                self.rx_data_size = data[0]  # Lấy kích thước dữ liệu thực tế từ byte đầu tiên
                self.rx_data = data[1:self.rx_data_size + 1]  # Bỏ padding

                # Kiểm tra xem đây có phải là dữ liệu chuỗi (nếu có byte không phải ký tự UTF-8, sẽ gây lỗi)
                try:
                    complete_message = bytes(self.rx_data).decode('utf-8')  # Giải mã UTF-8
                    print(f"Complete message received: {complete_message}")
                except UnicodeDecodeError:
                    print(f"Complete message received: {self.rx_data}")

                self.data_complete = True  # Đặt cờ hoàn thành
                return

            # Xử lý First Frame (Frame đầu tiên của dữ liệu lớn hơn 8 byte)
            if data[0] & 0xF0 == 0x10:
                print(f"Received First Frame: {data}")
                self.rx_data_size = ((data[0] & 0x0F) << 8) | data[1]  # Kích thước dữ liệu
                self.rx_data = data[2:8]  # Lưu dữ liệu từ First Frame
                self.received_blocks = 0  # Đặt lại số block đã nhận
                self.writeFlowControlFrame()  # Gửi ngay Flow Control Frame
                return

            # Xử lý Consecutive Frame (Các frame tiếp theo sau First Frame)
            if data[0] & 0xF0 == 0x20:
                # Lưu dữ liệu từ Consecutive Frame
                self.rx_data += data[1:8]  # Bỏ byte padding khi lắp ráp
                self.received_blocks += 1

                # Nếu nhận đủ dữ liệu theo kích thước ban đầu, ngừng xử lý
                if len(self.rx_data) >= self.rx_data_size:
                    self.rx_data = self.rx_data[:self.rx_data_size]  # Bỏ byte padding cuối cùng

                    # Kiểm tra xem đây có phải là dữ liệu chuỗi
                    try:
                        complete_message = bytes(self.rx_data).decode('utf-8')  # Giải mã UTF-8
                        print(f"Complete message received: {complete_message}")
                    except UnicodeDecodeError:
                        print(f"Complete message received: {self.rx_data}")

                    self.data_complete = True  # Đặt cờ khi dữ liệu đã hoàn thành
                    return  # Ngừng gửi Flow Control Frame

                # Nếu chưa nhận đủ dữ liệu, tiếp tục gửi Flow Control Frame
                if self.received_blocks % self.blk_size_for_rx == 0:
                    time.sleep(0.05)  # Dừng 50ms trước khi gửi Flow Control Frame để phản hồi kịp thời
                    self.writeFlowControlFrame()

                return

            # Xử lý Flow Control Frame (FC), chỉ xử lý nếu chưa hoàn thành
            if data[0] & 0xF0 == 0x30 and not self.data_complete:
                print(f"Received Flow Control Frame (FC): {data}")
                self.flow_ctrl_ok.set()


# ------------------- SETUP ------------------- #
bus1 = can.Bus('test', interface='virtual')
bus2 = can.Bus('test', interface='virtual')

# Sender node (tp1) - Transmitter
tp1 = CANTP(bus1, 0x727, 0x72F)

# Receiver node (tp2) - Receiver
tp2 = CANTP(bus2, 0x72F, 0x727)

can.Notifier(bus1, [tp1])
can.Notifier(bus2, [tp2])

# ------------------- TESTING ------------------- #
# Data to send
# data1 = "Hello my python project of CanTP this is the project about cantp"
data1 = "Hello Hello Hello Hello "
# data1 = [1,2,3,4,5,6,7,8,9,1,2,3,4,5,6,7,8,9,1,2,3,4,5,6] # VỚI DATA LÀ DSACH SỐ NGUYÊN THÌ OUTPUT ĐAG SAI
# Transmitting data
tp1.sendData(data1)

while True:
    time.sleep(1)


