from datetime import datetime
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from lxml import etree
from lxml.etree import QName
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from io import BytesIO
from PyQt5 import QtCore, QtWidgets
import os
import sys
import time
import av
av.logging.set_level(av.logging.PANIC) #shut up


class Stream:
    def __init__(self, stream_type, bitrate, codec, quality, base_url):
        self.stream_type = stream_type
        self.bitrate = bitrate
        self.codec = codec
        self.quality = quality
        self.base_url = base_url

    def __str__(self):
        return f"{self.quality:{' '}{'>'}{9}} Bitrate: {self.bitrate:{' '}{'>'}{8}} Codec: {self.codec}"


class Segment:
    def __init__(self, stream, seg_num):
        self.url = stream.base_url + str(seg_num)
        self.seg_num = seg_num
        self.data = BytesIO()
        self.success = False

# TODO multithreading na update progress baru
class Window(object):
    def __init__(self, Window):
        self.setupUi(Window)
        self.setupApp()
        self.last_pts = 0

    def setupApp(self):
        self.d_time = requests.Session()
        self.retry = Retry(connect=5, backoff_factor=0.5)
        self.adapter = HTTPAdapter(max_retries=self.retry)
        self.d_time.mount('http://', self.adapter)
        self.d_time.mount('https://', self.adapter)
        self.get = self.d_time.get

    def get_mpd_data(self, video_url):
        page = self.get(video_url).text
        if 'dashManifestUrl\\":\\"' in page:
            mpd_link = page.split('dashManifestUrl\\":\\"')[-1].split('\\"')[0].replace("\/", "/")
        elif 'dashManifestUrl":"' in page:
            mpd_link = page.split('dashManifestUrl":"')[-1].split('"')[0].replace("\/", "/")
        else:
            return None
        return self.get(mpd_link).text

    def create_combo_boxes(self, audio_list, video_list):
        self.audio_combo_box.clear()
        self.video_combo_box.clear()

        for i in range(len(audio_list)):
            self.audio_combo_box.addItem(str(audio_list[i]))

        for i in range(len(video_list)):
            self.video_combo_box.addItem(str(video_list[i]))

        self.create_cpu_threads_combo_box()

    def process_mpd(self, mpd_data):
        tree = etree.parse(BytesIO(mpd_data.encode()))
        root = tree.getroot()
        nsmap = {(k or "def"): v for k, v in root.nsmap.items()}
        time = root.attrib[QName(nsmap["yt"], "mpdResponseTime")]
        d_time = datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%f")
        total_seg = (
                int(root.attrib[QName(nsmap["yt"], "earliestMediaSequence")])
                + len(tree.findall(".//def:S", nsmap))
                - 1
        )
        seg_len = int(float(root.attrib["minimumUpdatePeriod"][2:-1]))
        attribute_sets = tree.findall(".//def:Period/def:AdaptationSet", nsmap)
        v_streams = []
        a_streams = []
        for a in attribute_sets:
            stream_type = a.attrib["mimeType"][0]
            for r in a.findall(".//def:Representation", nsmap):
                bitrate = int(r.attrib["bandwidth"])
                codec = r.attrib["codecs"]
                base_url = r.find(".//def:BaseURL", nsmap).text + "sq/"
                if stream_type == "a":
                    quality = r.attrib["audioSamplingRate"]
                    a_streams.append(Stream(stream_type, bitrate, codec, quality, base_url))
                elif stream_type == "v":
                    quality = f"{r.attrib['width']}x{r.attrib['height']}"
                    v_streams.append(Stream(stream_type, bitrate, codec, quality, base_url))
        a_streams.sort(key=lambda x: x.bitrate, reverse=True)
        v_streams.sort(key=lambda x: x.bitrate, reverse=True)
        return a_streams, v_streams, total_seg, d_time, seg_len


    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(500, 400)
        MainWindow.setWindowTitle("Aktif")

        # CENTRAL WIDGET
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        MainWindow.setCentralWidget(self.centralwidget)

        # BUTTONS
        self.pushButton = QtWidgets.QPushButton(self.centralwidget)
        self.pushButton.setGeometry(370, 35, 100, 30)
        self.pushButton.setObjectName("pushButton")
        self.pushButton.setText("Get info")

        self.check_button = QtWidgets.QPushButton(self.centralwidget)
        self.check_button.setGeometry(370, 210, 100, 30)
        self.check_button.setObjectName("check_button")
        self.check_button.setText("Check Info")
        self.check_button.setDisabled(True)

        self.download_button = QtWidgets.QPushButton(self.centralwidget)
        self.download_button.setGeometry(370, 250, 100, 30)
        self.download_button.setObjectName("download_button")
        self.download_button.setText("Download")
        self.download_button.setDisabled(True)


        # LABELS
        self.label = QtWidgets.QLabel(self.centralwidget)
        self.label.setGeometry(QtCore.QRect(10, 20, 71, 16))
        self.label.setObjectName("label1")
        self.label.setText("URL input")

        self.label_video_options = QtWidgets.QLabel(self.centralwidget)
        self.label_video_options.setGeometry(20, 85, 40, 16)
        self.label_video_options.setObjectName("label_video_options")
        self.label_video_options.setText("Video:")

        self.label_audio_options = QtWidgets.QLabel(self.centralwidget)
        self.label_audio_options.setGeometry(265, 85, 40, 16)
        self.label_audio_options.setObjectName("label_audio_options")
        self.label_audio_options.setText("Audio:")

        self.label_from_time = QtWidgets.QLabel(self.centralwidget)
        self.label_from_time.setGeometry(25, 140, 180, 16)
        self.label_from_time.setObjectName("label_from_time")
        self.label_from_time.setText("From time (keep this format):")

        self.label_to_time = QtWidgets.QLabel(self.centralwidget)
        self.label_to_time.setGeometry(25, 190, 180, 16)
        self.label_to_time.setObjectName("label_to_time")
        self.label_to_time.setText("To time (keep this format):")

        self.label_output_file = QtWidgets.QLabel(self.centralwidget)
        self.label_output_file.setGeometry(25, 240, 50, 16)
        self.label_output_file.setObjectName("label_output_file")
        self.label_output_file.setText("File:")

        self.label_thread_count = QtWidgets.QLabel(self.centralwidget)
        self.label_thread_count.setGeometry(265, 140, 65, 16)
        self.label_thread_count.setObjectName("label_thread_count")
        self.label_thread_count.setText("Threads:")

        self.labe_overwrite_file = QtWidgets.QLabel(self.centralwidget)
        self.labe_overwrite_file.setGeometry(380, 140, 65, 16)
        self.labe_overwrite_file.setObjectName("labe_overwrite_file")
        self.labe_overwrite_file.setText("Overwrite:")

        # TEXT INPUTS
        self.url_input = QtWidgets.QLineEdit(self.centralwidget)
        self.url_input.setGeometry(10, 40, 350, 20)
        #self.url_input.setText("")
        self.url_input.setObjectName("url_input")

        self.from_time_input = QtWidgets.QLineEdit(self.centralwidget)
        self.from_time_input.setGeometry(20, 160, 220, 20)
        self.from_time_input.setObjectName("from_time_input")
        self.from_time_input.setDisabled(True)
        self.from_time_input.setText("2021-02-01T08:00")

        self.to_time_input = QtWidgets.QLineEdit(self.centralwidget)
        self.to_time_input.setGeometry(20, 210, 220, 20)
        self.to_time_input.setObjectName("to_time_input")
        self.to_time_input.setDisabled(True)
        self.to_time_input.setText("2021-02-01T08:01")

        self.file_name_input = QtWidgets.QLineEdit(self.centralwidget)
        self.file_name_input.setGeometry(20, 260, 220, 20)
        self.file_name_input.setObjectName("file_name_input")
        self.file_name_input.setText("new_video")
        self.file_name_input.setDisabled(True)

        # COMBO BOXES
        self.video_combo_box = QtWidgets.QComboBox(self.centralwidget)
        self.video_combo_box.setGeometry(10, 100, 235, 35)
        self.video_combo_box.setObjectName("video_combo_box")
        self.video_combo_box.setDisabled(True)

        self.audio_combo_box = QtWidgets.QComboBox(self.centralwidget)
        self.audio_combo_box.setGeometry(245, 100, 235, 35)
        self.audio_combo_box.setObjectName("audio_combo_box")
        self.audio_combo_box.setDisabled(True)

        self.output_format_combo_box = QtWidgets.QComboBox(self.centralwidget)
        self.output_format_combo_box.setGeometry(255, 260, 90, 25)
        self.output_format_combo_box.setObjectName("output_format_combo_box")
        self.output_format_combo_box.setDisabled(True)
        self.output_format_combo_box.addItem(".mp4")
        self.output_format_combo_box.addItem(".mkv")

        self.thread_combo_box = QtWidgets.QComboBox(self.centralwidget)
        self.thread_combo_box.setGeometry(260, 157, 65, 25)
        self.thread_combo_box.setObjectName("thread_combo_box")
        self.thread_combo_box.setDisabled(True)

        # CHECK BOXES
        self.overwrite_check_box = QtWidgets.QCheckBox(self.centralwidget)
        self.overwrite_check_box.setGeometry(405, 160, 65, 16)

        # STATUS BAR
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)

        # PROGRESS BAR
        '''self.progress_bar = QtWidgets.QProgressBar(self.centralwidget)
        self.progress_bar.setObjectName("progress_bar")
        self.progress_bar.setGeometry(20, 300, 350, 80)
        self.progress_bar.setMaximum(5)
        self.progress_bar.setMinimum(0)
        self.progress_bar.setValue(0)
        self.progress_bar.show()'''

        # TRIGGERS
        self.pushButton.clicked.connect(self.get_download_options)
        self.thread_combo_box.currentIndexChanged.connect(lambda: self.change_thread_count(self.thread_combo_box.currentText()))
        self.check_button.clicked.connect(self.check_input_fields)
        self.download_button.clicked.connect(self.begin_download)

        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def get_download_options(self):
        url = self.url_input.text()
        if url != "":
            mpd_data = self.get_mpd_data(video_url=url)
            if mpd_data is not None:
                self.activate_input_fields()
                self.a_streams, self.v_streams, self.segment_count, self.d_time, self.segment_length = self.process_mpd(mpd_data)
                self.create_combo_boxes(self.a_streams, self.v_streams)
            else:
                self.deactivate_input_fields()

    def create_cpu_threads_combo_box(self):
        self.download_threads = cpu_count()
        for i in range(1, self.download_threads + 1):
            self.thread_combo_box.addItem(str(i))

    def change_thread_count(self, new_count):
        self.download_threads = new_count

    def activate_input_fields(self):
        self.audio_combo_box.setDisabled(False)
        self.video_combo_box.setDisabled(False)
        self.from_time_input.setDisabled(False)
        self.to_time_input.setDisabled(False)
        self.file_name_input.setDisabled(False)
        self.output_format_combo_box.setDisabled(False)
        self.file_name_input.setDisabled(False)
        self.check_button.setDisabled(False)
        self.thread_combo_box.setDisabled(False)

    def deactivate_input_fields(self):
        self.audio_combo_box.setDisabled(True)
        self.video_combo_box.setDisabled(True)
        self.from_time_input.setDisabled(True)
        self.to_time_input.setDisabled(True)
        self.file_name_input.setDisabled(True)
        self.output_format_combo_box.setDisabled(True)
        self.file_name_input.setDisabled(True)
        self.download_button.setDisabled(True)
        self.thread_combo_box.setDisabled(True)
        self.download_button.setDisabled(True)

    def parse_datetime(self, input_date):
        return datetime.strptime(input_date, "%Y-%m-%dT%H:%M")

    def check_input_fields(self):
        if self.parse_datetime(self.from_time_input.text()) != -1 and self.parse_datetime(self.to_time_input.text()) != -1 and self.file_name_input.text() != "" and self.file_name_input.text() != " " and self.check_if_exists(f"{self.file_name_input.text()}{self.output_format_combo_box.currentText()}"):
            self.download_button.setDisabled(False)
            self.start_time = self.parse_datetime(self.from_time_input.text())
            self.end_time = self.parse_datetime(self.to_time_input.text())

            e_dtime = self.parse_datetime(self.to_time_input.text())
            s_dtime = self.parse_datetime(self.from_time_input.text())
            self.duration = (e_dtime - s_dtime).total_seconds()

            self.start_segment = self.segment_count - round((self.d_time - self.start_time).total_seconds() / self.segment_length)
            if self.start_segment < 0:
                self.start_segment = 0

            self.end_segment = self.start_segment + round(self.duration / self.segment_length)

            if self.end_segment > self.segment_count:
                self.statusbar.showMessage("Error: You are requesting segments that dont exist yet!")

    def check_if_exists(self, output_name):

        if os.path.exists(output_name):
            if self.overwrite_check_box.isChecked():
                self.statusbar.clearMessage()
                self.statusbar.showMessage("Old file with the same name will be overwritten", 3000)
                os.remove(output_name)
                return True
            else:
                self.statusbar.clearMessage()
                self.statusbar.showMessage("File already exists", 3000)
                return False
        else:
            return True

    def begin_download(self):
        self.statusbar.showMessage("Downloading segments")

        video_format = self.video_combo_box.currentIndex()
        audio_format = self.audio_combo_box.currentIndex()

        v_data = self.download(self.v_streams[video_format], range(self.start_segment, self.end_segment), int(self.download_threads))
        a_data = self.download(self.a_streams[audio_format], range(self.start_segment, self.end_segment), int(self.download_threads))

        self.statusbar.showMessage("Muxing into file", 5000)

        self.mux_to_file(f"{self.file_name_input.text()}{self.output_format_combo_box.currentText()}", a_data, v_data)

    def download_func(self, seg):
        while True:
            req = self.get(seg.url)
            if req.status_code == 200:
                break
            time.sleep(0.5)
        return req.content

    def download(self, stream, seg_range, threads):
        segments = []
        for seg in seg_range:
            segments.append(Segment(stream, seg))

        #self.progress_bar.setMaximum(len(segments) - 1)
        #self.downloaded_parts = 0

        results = ThreadPool(threads).map(self.download_func, segments)
        combined_file = BytesIO()

        for res in results:
            combined_file.write(res)

        return combined_file

    def mux_to_file(self, output_file_name, aud, vid):
        vid.seek(0)
        aud.seek(0)
        video = av.open(vid, "r")
        audio = av.open(aud, "r")
        output = av.open(output_file_name, "w")
        v_in = video.streams.video[0]
        a_in = audio.streams.audio[0]

        video_p = video.demux()
        audio_p = audio.demux(a_in)

        output_video = output.add_stream(template=v_in)
        output_audio = output.add_stream(template=a_in)

        self.last_pts = 0
        self.step = 0

        for packet in video_p:
            if packet.dts is None:
                continue

            packet.dts = self.last_pts
            packet.pts = self.last_pts
            self.last_pts += packet.duration

            packet.stream = output_video
            output.mux(packet)

        self.last_pts = 0
        self.step = 0
        for packet in audio_p:
            if packet.dts is None:
                continue

            packet.dts = self.last_pts
            packet.pts = self.last_pts
            self.last_pts += packet.duration

            packet.stream = output_audio
            output.mux(packet)

        output.close()
        audio.close()
        video.close()
        self.statusbar.showMessage("Done!")

if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    ui = Window(MainWindow)
    MainWindow.show()
    sys.exit(app.exec_())