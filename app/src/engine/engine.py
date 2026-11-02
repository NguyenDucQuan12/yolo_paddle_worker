"""
Xử lý các công việc liên quan đến inference YOLO và PaddleOCR, bao gồm nạp model, xử lý ảnh/PDF, lưu kết quả metadata và ảnh crop.
"""
from contextlib import closing
import json
import math
import os
import time
import logging
import warnings
from importlib.metadata import version
from pathlib import Path
import numpy as np
from PIL import Image, ImageDraw, ImageOps
import pypdfium2 as pdfium
import torch
import paddle

# Thiết lập biến môi trường để vô hiệu hóa kiểm tra nguồn model của PaddleOCR và thay đổi đường dẫn lưu trữ model để sử dụng nội bộ.
os.environ["PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK"] = "true"
os.environ["PADDLE_PDX_CACHE_HOME"] = r"models" # Thay đổi đường dẫn lưu trữ model để dùng nội bộ
from ultralytics import YOLO
from paddleocr import PaddleOCRVL
from utils.resources import check_disk, release_cache

logger = logging.getLogger(__name__)

def write_json(path: Path, value):
    """
    Ghi dict hoặc list ra file JSON an toàn, thông qua file tạm 
    path phải là Path object, không phải str, để with_suffix() hoạt động đúng  
    Ví dụ: path=Path('data/result.json')
    """
    # Lấy đường dẫn file đích và tạo file tạm với hậu tố .tmp
    temp = path.with_suffix(path.suffix + '.tmp')   # data/result.json.tmp

    # mode 'w' (ghi) và encoding UTF-8. Nếu file tạm đã tồn tại, nó sẽ bị ghi đè.
    with temp.open('w', encoding='utf-8') as stream:
        # Chuyển thành JSON với indent=2 thụt vào 2 khoảng trắng để dễ đọc
        json.dump(value, stream, ensure_ascii=False, indent=2)
        # Đẩy dữ liệu từ bộ đệm Python xuống hệ thống file
        stream.flush()
        # Yêu cầu hệ điều hành ghi dữ liệu từ bộ đệm của hệ thống xuống đĩa
        os.fsync(stream.fileno())

    # Thay thế file gốc bằng file tạm. Nếu crash giữa chừng, file gốc vẫn còn nguyên.
    temp.replace(path)

def image_pages(source: Path, cfg: dict):
    """
    Nhận tài liệu PDF hoặc ảnh tĩnh rồi cung cấp tuần tự từng trang ảnh RGB, kèm metadata.
    """
    # Lấy cấu hình lưu trữ từ cfg, bao gồm các giới hạn về kích thước và số lượng trang.
    settings = cfg['storage']

    # Đọc file ở dạng nhị phân để kiểm tra xem có phải PDF hay không. Chỉ đọc 1024 byte đầu tiên để xác định định dạng.
    with source.open('rb') as stream:
        # Kiểm tra xem file có phải PDF hay không bằng cách tìm chuỗi b'%PDF-' trong 1024 byte đầu tiên. Nếu có, đây là file PDF; nếu không, đây là ảnh tĩnh (JPEG/PNG/WebP).
        is_pdf = b'%PDF-' in stream.read(1024)

    if is_pdf:
        with closing(pdfium.PdfDocument(source)) as document:
            # Kiểm tra PDF có ít nhất một trang và không vượt số trang cho phép.
            if not 1 <= len(document) <= settings['max_pdf_pages']:
                raise ValueError('PDF vượt giới hạn số trang hoặc không có trang')

            for index, page in enumerate(document):
                # Bitmap là ảnh RGB với kích thước được scale theo max_edge và max_pixels.
                bitmap = None
                try:
                    # Đọc chiều rộng và chiều cao của trang theo đơn vị PDF point.
                    width, height = page.get_size()

                    # Nếu kích thước trang PDF không hợp lệ
                    if not all(math.isfinite(v) and v > 0 for v in (width, height)):
                        raise ValueError('Kích thước trang PDF không hợp lệ')

                    # Tính tỉ lệ render để đáp ứng 2 giới hạn: max_edge (cạnh dài nhất) và max_pixels (tổng số pixel). scale sẽ là min giữa 2 giới hạn này.
                    scale = min(
                        settings['max_edge'] / max(width, height),
                        (settings['max_pixels'] / (width * height)) ** 0.5
                    )
                    # Chuyển trang PDF thành ảnh raster với tỷ lệ vừa tính.
                    # Ví dụ trang 1000 × 2000 point, scale=0.8, tạo bitmap khoảng 800 × 1600 pixel.
                    bitmap = page.render(scale=scale)
                    # Chuyển bitmap sang ảnh RGB
                    image = bitmap.to_pil().convert('RGB')

                    try:
                        # yield ảnh RGB để xử lý tiếp
                        yield image, {'page': index+1, 'pdf_size_points': [width, height]}
                    finally:
                        image.close()
                finally:
                    if bitmap is not None:
                        bitmap.close()
                    page.close()

    else:
        # Sử dụng warnings.catch_warnings() để biến cảnh báo DecompressionBombWarning của PIL thành lỗi. Điều này giúp tránh việc mở các ảnh quá lớn có thể gây tràn bộ nhớ.
        with warnings.catch_warnings():
            # Thiết lập cảnh báo DecompressionBombWarning thành lỗi để tránh mở các ảnh có số pixel quá lớn.
            warnings.simplefilter('error', Image.DecompressionBombWarning)

            # Mở file ảnh
            with Image.open(source) as original:
                # Ảnh có định dạng ảnh hợp lệ và chỉ có 1 frame (không phải ảnh động)
                if original.format not in {'JPEG', 'PNG', 'WEBP'} or getattr(original, 'n_frames', 1) != 1:
                    raise ValueError('Chỉ nhận JPEG, PNG, WebP tĩnh hoặc PDF')

                # Tính tổng pixel và so sánh với cấu hình
                if original.width * original.height > settings['max_pixels']:
                    raise ValueError('Số pixel ảnh vượt quá giới hạn cấu hình')

                # Chỉnh ảnh theo thông tin hướng lưu trong EXIF, sau đó chuyển thành RGB.
                # Ví dụ điện thoại lưu pixel theo hướng ngang nhưng EXIF báo cần xoay để hiển thị dọc
                image = ImageOps.exif_transpose(original).convert('RGB')
                # Lưu kích thước gốc của ảnh
                source_size = list(image.size)

                # Resize ảnh nếu cạnh dài nhất vượt quá max_edge, giữ tỉ lệ. thumbnail() sẽ không phóng to ảnh nhỏ.
                edge = settings['max_edge']
                image.thumbnail((edge, edge))

                # yield ảnh RGB và metadata cho ảnh tĩnh. Metadata bao gồm số trang (1) và kích thước gốc của ảnh sau khi áp dụng EXIF.
                try:
                    yield image, {'page': 1, 'source_size_after_exif': source_size}
                finally:
                    image.close()

class Engine:
    """
    Nạp model YOLO và PaddleOCR một lần, tái sử dụng cho nhiều job. Mỗi worker có một instance Engine riêng.
    """
    def __init__(self, cfg, worker):
        """
        Khởi tạo engine với cấu hình và thông tin worker.
        """
        # Yêu cầu cơ chế auto_growth của Paddle để tránh OOM khi GPU có nhiều process. Nếu không set, Paddle có thể chiếm toàn bộ VRAM.
        os.environ.setdefault('FLAGS_allocator_strategy', 'auto_growth')
        # Lưu cấu hình model và thông tin worker để sử dụng trong các phương thức khác.
        self.cfg, self.worker = cfg, worker
        self.torch, self.paddle = torch, paddle

        # Lấy cấu hình model từ cfg
        m = cfg['models']

        # chọn CPU hoặc GPU
        gpu_ok = torch.cuda.is_available() and paddle.is_compiled_with_cuda() and paddle.device.cuda.device_count() > 0
        if worker['device'] == 'gpu' and not gpu_ok:
            raise RuntimeError('Người dùng yêu cầu GPU nhưng Torch/Paddle chưa nhận được GPU')

        # Nếu GPU có sẵn và worker không yêu cầu CPU, sử dụng GPU; ngược lại, sử dụng CPU
        self.device = 'gpu' if gpu_ok and worker['device'] != 'cpu' else 'cpu'

        # Giới hạn số luồng CPU cho Torch để tránh chiếm dụng toàn bộ CPU.
        torch.set_num_threads(m['cpu_threads'])
        # Đặt Paddle vào GPU số 0 được nhìn thấy trong process này
        paddle.set_device('gpu:0' if self.device == 'gpu' else 'cpu')

        # Kiểm tra bộ nhớ trống trên GPU trước khi nạp model.
        self.memory_guard()

        # Nạp model YOLO từ đường dẫn cấu hình.
        self.yolo = YOLO(m['yolo_path'])
        # Kiểm tra task của YOLO phải là 'detect' vì bên dưới đang vẽ bounding box
        if self.yolo.task != 'detect':
            raise ValueError('YOLO model phải thuộc task detect')
        # Đưa model YOLO lên GPU hoặc CPU tùy theo cấu hình worker
        self.yolo.to('cuda:0' if self.device == 'gpu' else 'cpu')

        # Cấu hình PaddleOCR với các tham số từ cfg. Điều này giúp OCR hoạt động đúng với ngôn ngữ và phiên bản mong muốn.
        options = dict(
            # lang=m['ocr_lang'],
            device='gpu:0' if self.device == 'gpu' else 'cpu',
            # det_model_dir='./models/PP-server_det_infer/',
            # rec_model_dir='./models/PP-server_rec_infer/',
            # cls_model_dir='./models/PP-cls_infer/',
            # use_angle_cls=True,
        )

        # Khởi tạo PaddleOCR với các tùy chọn đã thiết lập
        self.ocr = PaddleOCRVL(**options)

        # Tạo 1 ảnh nhỏ 640x640 màu trắng để kiểm tra inference. Điều này giúp "warmup" các model trước khi nhận job thực tế, giảm thời gian inference đầu tiên.
        warmup = np.full((640, 640, 3), 255, dtype=np.uint8)

        # Gọi detect và recognize trên ảnh warmup để nạp model vào GPU/CPU và tránh OOM khi inference thực tế. Sau đó, xóa warmup để giải phóng bộ nhớ.
        self.detect(warmup)
        self.recognize(warmup)
        del warmup
        self.release()

        # Ghi log thông tin model đã nạp, bao gồm phiên bản Torch, PaddleOCR và Ultralytics. Điều này giúp theo dõi runtime thực tế của worker.
        logger.info("Worker %s nạp model xong: Torch %s, PaddleOCR %s, Ultralytics %s, device=%s",
            worker['id'],
            version('torch'),
            version('paddleocr'),
            version('ultralytics'),
            self.device
        )

    def memory_guard(self):
        """
        Kiểm tra VRAM trống trên GPU trước khi nạp model.  
        Nếu VRAM trống thấp hơn ngưỡng cấu hình, raise MemoryError để tránh OOM.
        """
        if self.device == 'gpu':
            # Lấy số byte trống và tổng VRAM trên GPU số 0.
            free, total = self.torch.cuda.mem_get_info(0)
            if free < self.cfg['models']['min_free_gpu_mb'] * 1024 * 1024:
                raise MemoryError('VRAM trống thấp hơn thông số cấu hình, có thể OOM khi nạp model')
            return {'gpu_free_mb': round(free/2**20), 'gpu_total_mb': round(total/2**20)}

        return {}

    def release(self):
        """
        Giải phóng tài nguyên sử dụng bởi model.
        """
        # Lấy predictor của YOLO nếu có. Ultralytics có thể giữ ảnh/result của lần chạy cuối, nên cần xóa các tham chiếu tạm để tránh giữ dữ liệu không cần thiết.
        predictor = getattr(self.yolo, 'predictor', None)
        if predictor is not None:
            # Duyệt các thuộc tính có thể giữ dữ liệu của lần dự đoán trước.
            for name in ('results', 'batch', 'plotted_img', 'dataset'):
                if hasattr(predictor, name):
                    setattr(predictor, name, None)

        # Giải phóng cache GPU và thu gom Python objects sau khi inference xong.
        release_cache(
            self.torch,
            self.paddle,
            self.device == 'gpu'
            and self.cfg['models']['release_cache_after_job']
        )

    def detect(self, bgr):
        """
        Thực hiện inference YOLO trên ảnh BGR, trả về danh sách detection.
        """
        # Chạy YOLO ở chế độ suy luận, không xây dựng đồ thị gradient như khi huấn luyện.
        with self.torch.inference_mode():
            result = self.yolo.predict(
                bgr,                                                # Ảnh cần nhận diện, định dạng numpy BGR.
                conf=self.cfg['models']['confidence'],              # Ngưỡng confidence để lọc detection, ví dụ 0.25.
                imgsz=self.cfg['models']['yolo_imgsz'],             # Kích thước ảnh đầu vào cho YOLO, ví dụ 640. YOLO sẽ resize ảnh về kích thước này trước khi inference.
                max_det=self.cfg['storage']['max_crops_per_page'],  # Giới hạn số detection tối đa để tránh bùng nổ số box. Nếu có nhiều hơn, YOLO sẽ chỉ trả về max_det box có confidence cao nhất.
                device=0 if self.device == 'gpu' else 'cpu',        # Chỉ định device cho YOLO: 0 là GPU:0, 'cpu' là CPU. Điều này giúp inference trên GPU nếu có, hoặc CPU nếu không.
                verbose=False                                       # Tắt log chi tiết của YOLO để tránh in ra quá nhiều thông tin trong console. Chỉ in log quan trọng.
            )[0]                                                    # Lấy kết quả đầu tiên trong list result, vì YOLO có thể trả về nhiều kết quả nếu input là batch. Ở đây chúng ta chỉ inference 1 ảnh nên lấy result[0].

            # Lấy bounding boxes từ kết quả YOLO
            boxes = result.boxes
            # Nếu không có box nào được phát hiện, trả về danh sách rỗng.
            if boxes is None:
                return []

            # x1y1x2y2: là mép trái trên và phải dưới của box, confidence score, class id. Chuyển sang list of dict để dễ sử dụng.
            return [
                {
                    'class_id': int(c),
                    'label': result.names[int(c)],
                    'confidence': float(s),
                    'xyxy': [float(v) for v in xy]
                } for xy, s, c in zip(
                    boxes.xyxy.cpu().tolist(),
                    boxes.conf.cpu().tolist(),
                    boxes.cls.cpu().tolist()
                )
            ]

    def recognize_(self, bgr):
        """
        Thực hiện OCR trên ảnh BGR, trả về danh sách line.  
        Dành cho PaddleOCR 3.x, trả về danh sách dict với keys: text, confidence, polygon.  
        Đổi tên hàm sang recognize() để tránh trùng với phương thức recognize() của PaddleOCRVL.
        """
        # Danh sách rỗng để lưu kết quả OCR.
        lines = []
        # Chạy PaddleOCR trên ảnh BGR
        for result in self.ocr.predict(bgr):
            # Lấy các thông tin từ kết quả OCR: texts (danh sách chuỗi văn bản), scores (điểm nhận diện), polygons (vùng chứa văn bản)
            texts, scores, polygons = result['rec_texts'], result['rec_scores'], result['rec_polys']
            # Kiểm tra schema của kết quả OCR có khớp không. Nếu số lượng texts, scores và polygons không bằng nhau, raise RuntimeError để báo lỗi.
            if not len(texts) == len(scores) == len(polygons):
                raise RuntimeError('Schema PaddleOCR không khớp')

            # Chuẩn hóa kết quả OCR thành list dict với keys: text, confidence, polygon. polygon được chuyển thành list of float.
            lines.extend(
                {
                    'text': str(t),
                    'confidence': float(s),
                    'polygon': np.asarray(p).astype(float).tolist()
                }
                for t, s, p in zip(texts, scores, polygons)
            )
        return lines
    
    def recognize(self, bgr):
        """
        Thực hiện OCR trên ảnh BGR, trả về danh sách line.  
        Dành cho PaddleOCRVL, trả về danh sách dict với keys: text, confidence, polygon.
        """
        lines = []

        for result in self.ocr.predict(bgr):
            parsing_res_list = result.get("parsing_res_list", [])

            for block in parsing_res_list:
                # PaddleOCRVL trả về object PaddleOCRVLBlock
                text = getattr(block, "content", "")
                bbox = getattr(block, "bbox", None)
                polygon = getattr(block, "polygon_points", None)

                if not text or not str(text).strip():
                    continue

                # Nếu không có polygon thì chuyển bbox thành polygon 4 điểm
                if polygon is None and bbox is not None:
                    x1, y1, x2, y2 = map(float, bbox)
                    polygon = [
                        [x1, y1],
                        [x2, y1],
                        [x2, y2],
                        [x1, y2],
                    ]

                if polygon is None:
                    continue

                lines.append({
                    "text": str(text),
                    # PaddleOCRVL không cung cấp confidence cho từng block
                    "confidence": None,
                    "polygon": np.asarray(polygon, dtype=float).tolist(),
                })

        return lines

    def process(self, job, folder):
        """
        Xử lý một tài liệu, tuần tự từng trang, lưu metadata và ảnh crop.
        """
        # Danh sách chứa kết quả metadata của từng trang. Mỗi phần tử là dict với keys: page, width, height, detections, ocr, timing.
        pages = []
        # Lấy đường dẫn tới file nguồn từ cấu hình storage và job. job['source'] là đường dẫn tương đối trong thư mục root.
        source = Path(self.cfg['storage']['root']) / job['source']

        # Lần lượt xử lý từng trang ảnh từ nguồn (PDF hoặc ảnh tĩnh) bằng hàm image_pages. Hàm này yield từng ảnh RGB và metadata của trang.
        for image, metadata in image_pages(source, self.cfg):
            # Kiểm tra dung lượng đĩa trước khi lưu kết quả
            check_disk(self.cfg)
            # Lấy số trang hiện tại từ metadata. Ví dụ: metadata['page'] = 1, 2, 3, ...
            index = metadata['page']
            # Tạo thư mục riêng cho từng trang trong thư mục job. Ví dụ: folder/page_0001, folder/page_0002, ...
            page_dir = folder / f'page_{index:04d}'
            page_dir.mkdir(parents=True, exist_ok=True)

            # Ghi log cho từng trang
            logger.info("📝 Xử lý trang %d/%d của job %s", index, metadata.get('total_pages', '?'), job['id'])

            # Lưu ảnh normalized (sau khi áp dụng EXIF và resize) để debug. Lưu với chất lượng 95 để giảm dung lượng file mà vẫn giữ chất lượng tốt.
            image.save(page_dir / 'normalized.jpg', quality=95)
            # Đổi ảnh PIL RGB sang numpy BGR để phù hợp với input của YOLO và PaddleOCR.
            bgr = np.asarray(image)[:, :, ::-1].copy()

            # Khởi tạo danh sách rỗng cho detections, lines và dict rỗng cho timing. Detections là kết quả YOLO, lines là kết quả OCR, timing lưu thời gian inference.
            detections, lines, timing = [], [], {}

            # Nếu job yêu cầu task 'detect' hoặc 'analyze', thực hiện YOLO detection.
            if job['task'] in {'detect', 'analyze'}:
                # Đo thời gian inference YOLO để lưu vào timing. Sử dụng time.perf_counter() để đo thời gian chính xác hơn.
                start = time.perf_counter()
                detections = self.detect(bgr)
                timing['yolo_ms'] = round((time.perf_counter()-start)*1000, 2)

                logger.info("Hoàn thành phát hiện vật thể trong job %s, trang %d: %d detection(s), thời gian %0.2f ms", job['id'], index, len(detections), timing['yolo_ms'])

            # Nếu job yêu cầu task 'ocr' hoặc 'analyze', thực hiện OCR.
            if job['task'] in {'ocr', 'analyze'}:
                # Đo thời gian inference OCR để lưu vào timing. Sử dụng time.perf_counter() để đo thời gian chính xác hơn.
                start = time.perf_counter()
                lines = self.recognize(bgr)
                timing['ocr_ms'] = round((time.perf_counter()-start)*1000, 2)

                # Ghi log kết quả OCR
                logger.info("Hoàn thành nhận diện văn bản trong job %s, trang %d: %d line(s), thời gian %0.2f ms", job['id'], index, len(lines), timing['ocr_ms'])

            # Bỏ tham chiếu tới mảng numpy BGR sau khi suy luận xong
            del bgr
            # Lưu ảnh box YOLO và box OCR, cũng như các ảnh crop của từng detection và line.
            self.save_visuals(image, detections, lines, page_dir)

            # Tạo kết quả cho từng trang
            item = dict(
                **metadata,
                width=image.width,
                height=image.height,
                detections=detections,
                ocr=lines,
                timing=timing
            )

            # Lưu metadata của từng trang ra file JSON.
            write_json(page_dir / 'result.json', item)
            # Lưu toàn bộ text của trang ra file text.txt, mỗi line là một dòng văn bản từ OCR. Sử dụng UTF-8 để hỗ trợ Unicode.
            (page_dir / 'text.txt').write_text('\n'.join(line['text'] for line in lines), encoding='utf-8')

            # Thêm metadata của trang này vào danh sách pages để lưu dữ liệu của toàn bộ tài liệu sau khi xử lý xong.
            pages.append(item)
            # Giải phóng bộ nhớ sau khi xử lý xong từng trang để tránh giữ dữ liệu không cần thiết trong RAM/VRAM.
            self.release()
            # Ghi log cho từng trang đã xử lý xong
            logger.info("Hoàn thành xử lý trang %d/%d của job %s", index, metadata.get('total_pages', '?'), job['id'])

        # Lưu kết quả toàn bộ tài liệu ra file result.json trong thư mục job. Bao gồm job_id, worker_id, device, coordinate_system và danh sách pages.
        write_json(
            folder / 'result.json',
            {
                'job_id': job['id'],
                'worker_id': self.worker['id'],
                'device': self.device,
                'coordinate_system':
                    'pixels of normalized image; EXIF corrected and resized',
                'pages': pages
            }
        )
        # Lưu toàn bộ text của tài liệu ra file text.txt, mỗi trang được phân cách bằng form-feed (\f). Sử dụng UTF-8 để hỗ trợ Unicode.
        (folder / 'text.txt').write_text(
            '\n\f\n'.join(
                '\n'.join(line['text'] for line in p['ocr'])
                for p in pages
            ),
            encoding='utf-8'
        )

    def save_visuals(self, image, detections, lines, folder):
        """
        Lưu ảnh box YOLO và box OCR, cũng như các ảnh crop của từng detection và line.
        """
        # Tạo 1 bản sao của ảnh gốc để vẽ bounding box YOLO
        canvas = image.copy()
        # Tạo công cụ vẽ trên canvas để vẽ bounding box và text. Sử dụng ImageDraw từ PIL.
        draw = ImageDraw.Draw(canvas)
        # Tạo thư mục crops trong thư mục page để lưu các ảnh crop của từng detection và line.
        crop_dir = folder / 'crops'
        crop_dir.mkdir(exist_ok=True)

        # Duyệt qua từng vật thể mà YOLO phát hiện được, vẽ bounding box và lưu ảnh crop của từng vật thể.
        for index, box in enumerate(detections):
            # Đưa tọa xyxy của box vào hàm clamp để giới hạn trong kích thước ảnh và bỏ box rỗng/không hợp lệ.
            coords = self.clamp(box['xyxy'], image.size)
            # Bỏ box rỗng/không hợp lệ.
            if coords is None:
                continue

            # Vẽ bounding box YOLO màu đỏ với độ dày 3 pixel và ghi số thứ tự index lên góc trên bên trái của box
            draw.rectangle(coords, outline='red', width=3)
            draw.text((coords[0], coords[1]), str(index), fill='red')

            # Nếu số lượng crop chưa vượt quá max_crops_per_page, lưu ảnh crop của từng vật thể
            with image.crop(coords) as crop:
                crop.save(crop_dir / f'object_{index:04d}.jpg', quality=95)

        # Lưu ảnh box YOLO ra file yolo_boxes.jpg trong thư mục page. Chất lượng 95 để giảm dung lượng file mà vẫn giữ chất lượng tốt.
        canvas.save(folder / 'yolo_boxes.jpg', quality=95)
        canvas.close()

        # Tạo một bản sao khác của ảnh gốc để vẽ polygon OCR
        canvas = image.copy()
        draw = ImageDraw.Draw(canvas)
        for index, line in enumerate(lines):
            # Chuyển polygon thành các cặp tọa độ mà công cụ vẽ sử dụng.
            points = [(float(x), float(y)) for x, y in line['polygon']]
            if not points:
                continue

            draw.line(points + [points[0]], fill='blue', width=2)
            draw.text(points[0], str(index), fill='blue')
            coords = self.clamp([min(x for x,y in points), min(y for x,y in points), max(x for x,y in points), max(y for x,y in points)], image.size)
            if coords and index < self.cfg['storage']['max_crops_per_page']:
                with image.crop(coords) as crop:
                    crop.save(crop_dir / f'text_{index:04d}.jpg', quality=95)

        # Lưu ảnh box OCR ra file ocr_boxes.jpg trong thư mục page. Chất lượng 95 để giảm dung lượng file mà vẫn giữ chất lượng tốt.
        canvas.save(folder / 'ocr_boxes.jpg', quality=95)
        canvas.close()

    @staticmethod
    def clamp(coords, size):
        """
        Giới hạn tọa độ xyxy trong kích thước ảnh, bỏ box rỗng/không hợp lệ. Nếu box bị cắt hết ra ngoài ảnh, trả về None.  
        coords: (x1, y1, x2, y2) là tọa độ trái trên và phải dưới của box float.  
        size: (width, height) là kích thước của ảnh.  
        """
        x1,y1,x2,y2 = coords
        w,h = size
        result = (
            max(0, int(x1)),
            max(0, int(y1)),
            min(w, math.ceil(x2)),
            min(h, math.ceil(y2))
        )
        return result if result[2] > result[0] and result[3] > result[1] else None
