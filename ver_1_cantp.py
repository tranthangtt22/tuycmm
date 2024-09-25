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
        # msg += [0x00] * (8 - len(msg))  # Thêm padding nếu cần để đủ 8 byte
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

        # self.seq = (self.seq + 1) % 16
        # msg = [0x20 | self.seq] + data[:7]
        # print(f"Sending Consecutive Frame: {msg}")
        # self.sendMessage(msg)
        # return data[7:]

    # Write Flow Control (FC)
    def writeFlowControlFrame(self):
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
        


        # if len(data) <= 7:
        #     self.writeSingleFrame(data)
        # else:
        #     th = Thread(target=self.writeMultiFrame, args=(data,))
        #     th.start()
        #     th.join()  # In production remove this line for better performance


    # Receive message
    def on_message_received(self, msg):
        can_id = msg.arbitration_id
        data = list(msg.data) # Chuyển bytearray thành list để hiển thị tương tự như Sending

        if can_id == self.rxid:
            # Xử lý Single Frame (Dữ liệu nhỏ hơn 8 byte)
            if data[0] & 0xF0 == 0x00:
                print(f"Received Single Frame: {data}")
                self.rx_data_size = data[0]  # Lấy kích thước dữ liệu thực tế từ byte đầu tiên
                self.rx_data = data[1:self.rx_data_size + 1]  # Bỏ padding
                
                
                print(f"Complete message received: {self.rx_data}")
                return
            
                # print(f"Received Single Frame: {data}")
                # self.rx_data_size = data[0]
                # self.rx_data = data[1:self.rx_data_size + 1]
                # print(f"Complete message received: {self.rx_data}")
                # return
            
            # Xử lý First Frame (Frame đầu tiên của dữ liệu lớn hơn 8 byte
            if data[0] & 0xF0 == 0x10:
                print(f"Received First Frame: {data}")
                self.rx_data_size = ((data[0] & 0x0F) << 8) | data[1]  # Kích thước dữ liệu
                self.rx_data = data[2:8]  # Lưu dữ liệu từ First Frame
                self.writeFlowControlFrame()  # Gửi ngay Flow Control Frame
                return
            
            # Xử lý Consecutive Frame (Các frame tiếp theo sau First Frame)
            if data[0] & 0xF0 == 0x20:
                #Lưu dữ liệu từ Consecutive Frame
                self.rx_data += data[1:8] # Bỏ byte padding khi lắp ráp
                self.received_blocks += 1

                # Sau khi nhận đủ 3 Consecutive Frame, gửi lại Flow Control Frame
                if self.received_blocks % self.blk_size_for_rx == 0:
                    time.sleep(0.05)  # Dừng 50ms trước khi gửi Flow Control Frame để phản hồi kịp thời
                    self.writeFlowControlFrame()

                # Nếu nhận đủ dữ liệu theo kích thước ban đầu, in thông báo hoàn thành
                if len(self.rx_data) >= self.rx_data_size:
                    self.rx_data = self.rx_data[:self.rx_data_size]  # Bỏ byte padding cuối cùng
                    print(f"Complete message received: {self.rx_data}")
                    # print(f"Complete message received: {self.rx_data[:self.rx_data_size]}")
                return
            
            # Xử lý Flow Control Frame (Frame điều khiển luồng)
            if data[0] & 0xF0 == 0x30:
                flow_status = data[0] & 0x0F
                block_size = data[1]
                st_min = data[2]

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
# data1 = [1,2,3,4,5,6,7,8,9,10,11]
# data1 = [1,2,3]
# data1 = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74]
# data2 = [0x0A, 0x0B, 0x0C]
data1 = "mmmmmmmmmcmcmcmcmcmcmcmcmcm"
# data1 = [1,2,3,4,5,6,7]
# Transmitting data
tp1.sendData(data1)
# tp1.sendData(data2)

while True:
    time.sleep(1)
