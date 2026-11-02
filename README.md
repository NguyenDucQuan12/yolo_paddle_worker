# Tạo bảng

```SQL
-- Các lỗi thực thi thuộc phạm vi XACT_ABORT sẽ làm hủy transaction.
SET XACT_ABORT ON;

-- Không gửi các thông báo phụ như "(1 row affected)".
SET NOCOUNT ON;

-- Các tùy chọn cần thiết khi tạo và cập nhật bảng có filtered index.
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET NUMERIC_ROUNDABORT OFF;


-- Không cho tạo bảng ứng dụng trong database hệ thống.
IF DB_NAME() IN (N'master', N'model', N'msdb', N'tempdb')
BEGIN
    THROW 51010, N'Hãy chọn database ứng dụng trước khi chạy script.', 1;
END;


IF OBJECT_ID(N'Queue_job.QueueSettings', N'U') IS NOT NULL
   OR OBJECT_ID(N'Queue_job.Jobs', N'U') IS NOT NULL
BEGIN
    THROW 51012, N'Bảng Queue_job đã tồn tại. Không chạy lại script tạo mới.', 1;
END;

BEGIN TRY
    -- Tạo toàn bộ cấu trúc trong cùng một transaction.
    BEGIN TRANSACTION;

    -- Tạo schema nếu chưa tồn tại.
    IF SCHEMA_ID(N'Queue_job') IS NULL
    BEGIN
        EXEC(N'CREATE SCHEMA [Queue_job] AUTHORIZATION [dbo];');
    END;


    -- =========================================================
    -- BẢNG 1: CẤU HÌNH HÀNG ĐỢI
    -- =========================================================

    CREATE TABLE [Queue_job].[QueueSettings]
    (
        -- Bảng cấu hình chỉ sử dụng ID bằng 1. tinyint là số nguyên từ 0 đến 255
        [id] tinyint NOT NULL,
        -- Tổng số job queued + running được phép tồn tại.
        [max_pending] int NOT NULL,
        -- Tổng số lần nhận chạy tối đa theo cơ chế recovery.
        [max_attempts] int NOT NULL,
        -- Phiên bản cấu trúc dữ liệu mà Python hỗ trợ.
        [schema_version] int NOT NULL,

        -- Không cho trùng ID của dòng cấu hình.
        CONSTRAINT [PK_Queue_job_QueueSettings]
            PRIMARY KEY CLUSTERED ([id]),

        -- Chỉ cho phép ID bằng 1.
		-- Kết hợp với khóa chính, bảng có tối đa một dòng, vì không thể thêm dòng id=2 và cũng không thể thêm dòng id=1 thứ hai
        CONSTRAINT [CK_Queue_job_QueueSettings_SingleRow]
            CHECK ([id] = 1),

        -- Giới hạn queue phải lớn hơn 0.
        CONSTRAINT [CK_Queue_job_QueueSettings_MaxPending]
            CHECK ([max_pending] > 0),

        -- Phải cho phép ít nhất một lần chạy.
        CONSTRAINT [CK_Queue_job_QueueSettings_MaxAttempts]
            CHECK ([max_attempts] > 0),

        -- Giữ phiên bản Schema = 1, vì python đang chạy với version = 1
        CONSTRAINT [CK_Queue_job_QueueSettings_SchemaVersion]
            CHECK ([schema_version] = 1)
    );


    -- Tạo cấu hình ban đầu.
    INSERT INTO [Queue_job].[QueueSettings]
    (
        [id],
        [max_pending],
        [max_attempts],
        [schema_version]
    )
    VALUES
    (
        1,
        100,
        3,
        1
    );


    -- =========================================================
    -- BẢNG 2: HÀNG ĐỢI VÀ THÔNG TIN JOB
    -- =========================================================

    CREATE TABLE [Queue_job].[Jobs]
    (
        -- Số thứ tự tự tăng, phục vụ chọn job theo thứ tự đến.
        [seq] bigint IDENTITY(1, 1) NOT NULL,
        -- ID public do Python tạo bằng uuid.uuid4().hex.
        [id] varchar(32)
            COLLATE Latin1_General_100_BIN2 NOT NULL,
        -- Trạng thái hiện tại của job.
        [status] varchar(12) NOT NULL,
        -- Worker được chỉ định; NULL nghĩa là job dùng chung.
        [target] nvarchar(128)
            COLLATE Latin1_General_100_BIN2 NULL,
        -- Worker sở hữu lần chạy hiện tại.
        [owner] nvarchar(128)
            COLLATE Latin1_General_100_BIN2 NULL,
        -- Token riêng của mỗi lần nhận job.
        [claim_token] uniqueidentifier NULL,
        -- Loại công việc: detect, ocr hoặc analyze.
        [task] varchar(16) NOT NULL,
        -- Đường dẫn tương đối đến file nguồn.
        [source] nvarchar(1024) NOT NULL,
        -- Tên file gốc do người dùng tải lên.
        [original_name] nvarchar(255) NOT NULL,
        -- Thời điểm tạo job, dùng giờ UTC.
        [created] datetime2(7) NOT NULL
            CONSTRAINT [DF_Queue_job_Jobs_Created]
            DEFAULT (SYSUTCDATETIME()),
        -- Thời điểm bắt đầu lần chạy hiện tại.
        [started] datetime2(7) NULL,
        -- Thời điểm kết thúc job.
        [finished] datetime2(7) NULL,
        -- Số lần job đã được worker nhận chạy.
        [attempt] int NOT NULL
            CONSTRAINT [DF_Queue_job_Jobs_Attempt]
            DEFAULT (0),
        -- Nội dung lỗi; NULL khi không có lỗi.
        [error] nvarchar(2048) NULL,
        -- JSON lưu danh sách đường dẫn kết quả.
        [artifacts] nvarchar(max) NOT NULL
            CONSTRAINT [DF_Queue_job_Jobs_Artifacts]
            DEFAULT (N'[]'),


        -- Khóa chính dùng số thứ tự tăng dần.
        CONSTRAINT [PK_Queue_job_Jobs]
            PRIMARY KEY CLUSTERED ([seq]),

        -- Job ID không được trùng.
        CONSTRAINT [UQ_Queue_job_Jobs_Id]
            UNIQUE NONCLUSTERED ([id]),

        -- Chỉ cho phép bốn trạng thái đã định nghĩa.
        CONSTRAINT [CK_Queue_job_Jobs_Status]
            CHECK
            (
                [status] IN
                (
                    'queued',
                    'running',
                    'succeeded',
                    'failed'
                )
            ),

        -- Chỉ cho phép ba loại tác vụ hiện tại.
        CONSTRAINT [CK_Queue_job_Jobs_Task]
            CHECK
            (
                [task] IN ('detect', 'ocr', 'analyze')
            ),

        -- Số lần chạy không được âm.
        CONSTRAINT [CK_Queue_job_Jobs_Attempt]
            CHECK ([attempt] >= 0),

        -- Manifest kết quả phải là JSON hợp lệ.
        CONSTRAINT [CK_Queue_job_Jobs_ArtifactsJson]
            CHECK (ISJSON([artifacts]) = 1),

        -- Kiểm tra tính nhất quán giữa trạng thái và metadata.
        CONSTRAINT [CK_Queue_job_Jobs_State]
            CHECK
            (
                (
                    [status] = 'queued'
                    AND [owner] IS NULL
                    AND [claim_token] IS NULL
                    AND [started] IS NULL
                    AND [finished] IS NULL
                )
                OR
                (
                    [status] = 'running'
                    AND [owner] IS NOT NULL
                    AND [claim_token] IS NOT NULL
                    AND [started] IS NOT NULL
                    AND [finished] IS NULL
                )
                OR
                (
                    [status] IN ('succeeded', 'failed')
                    AND [finished] IS NOT NULL
                )
            )
    );


    -- Hỗ trợ tìm job theo trạng thái và thứ tự đến.
    CREATE NONCLUSTERED INDEX [IX_Queue_job_Jobs_Status_Seq]
        ON [Queue_job].[Jobs] ([status], [seq])
        INCLUDE ([target], [owner]);

    -- Mỗi worker chỉ được có tối đa một job running.
    CREATE UNIQUE NONCLUSTERED INDEX [UX_Queue_job_Jobs_RunningOwner]
        ON [Queue_job].[Jobs] ([owner])
        WHERE [status] = 'running';

    -- Hỗ trợ tìm job đã hết thời hạn lưu trữ.
    CREATE NONCLUSTERED INDEX [IX_Queue_job_Jobs_Finished]
        ON [Queue_job].[Jobs] ([finished])
        INCLUDE ([id], [status]);

    -- Xác nhận toàn bộ schema, bảng và index đã tạo thành công.
    COMMIT TRANSACTION;

END TRY
BEGIN CATCH

    -- Nếu còn transaction thì hủy những thay đổi chưa commit.
    IF XACT_STATE() <> 0
    BEGIN
        ROLLBACK TRANSACTION;
    END;

    -- Trả nguyên lỗi cho công cụ triển khai hoặc Python.
    THROW;

END CATCH;
```

## Bảng QueueSettings
Bảng `QueueSettings` là bảng chứa cấu hình hàng đợi cho các công việc cần xử lý  

Ví dụ với trường `max_pending = 100`:  
| Job đang chờ | Job đang chạy | Tổng | Có thể thêm job? |
|---:|---:|---:|---|
| 90 | 4 | 94 | Có |
| 96 | 4 | 100 | Không |

Ta có trường `max_attempts` là tổng số lần chạy tối đa theo cơ chế `recovery`  
Ví dụ `3`:  
1. Worker nhận lần đầu: `attempt=1`.
2. Worker chết, recovery cho thử lại.
3. Worker nhận lần hai: `attempt=2`.
4. Nếu tiếp tục lỗi tiến trình, có thể nhận lần ba.
5. Lần ba cũng bị gián đoạn thì recovery chuyển thành `failed`.

Ta đặt khóa chính là trường `id`  
```SQL
CONSTRAINT [PK_Queue_job_QueueSettings]
    PRIMARY KEY CLUSTERED ([id])
```
- Không cho hai dòng cùng ID.
- Không cho ID thiếu giá trị.
- CLUSTERED: tổ chức dữ liệu của bảng theo khóa index này.

Kết hợp với khóa chính, bảng có tối đa một dòng, vì không thể thêm dòng `id=2` và cũng không thể thêm dòng `id=1` thứ hai.
```SQL
CHECK ([id] = 1)
```

Trong python ta đang chạy cứng lệnh:  
```python
if settings['schema_version'] != 1:
    raise RuntimeError('Unsupported SQL schema version')
```
Vì vậy các phiên bản khác 1 đều không hợp lệ.  
```SQL
CHECK ([max_pending] > 0)
CHECK ([max_attempts] > 0)
CHECK ([schema_version] = 1)
```
Khi đó:  
```bash
max_pending = 0  → bị từ chối
max_attempts = -1 → bị từ chối
schema_version = 2 → bị từ chối trong phiên bản này
```

## Bảng Jobs
Một `job` được mô tả là công việc và các thông tin cần thiết mà từng `worker` sẽ dựa vào đó để xử lý.  
Một job mới có thể có dữ liệu như sau:  
```bash
seq           = 101
id            = aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
status        = queued
target        = NULL
owner         = NULL
claim_token   = NULL
task          = analyze
source        = jobs/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/original.bin
original_name = hóa_đơn.pdf
attempt       = 0
artifacts     = []
```
Ta có cột `seq` là thứ tự nội bộ để sắp xếp công việc nào mới, công việc nào cũ. Giá trị bắt đầu bằng `1` và mỗi lần cấp giá trị tăng `1`  
Cột `id` là mã công việc được tạo ra từ `uuid.uuid4().hex` với độ dài 32 ký tự. Ví dụ: `a36fa119b42a49dc89f27592ff91d487`  
```SQL
[id] varchar(32)
            COLLATE Latin1_General_100_BIN2 NOT NULL,
```
Ta sử dụng `BIN2` vì dùng quy tắc so sánh nhị phân, cần phân biệt chữ hoa và chữ thường.  
Cột `status` là trạng thái của công việc, ta ràng buộc nó với bốn giá trị sau  
| Giá trị | Ý nghĩa |
|---|---|
| `queued` | Đang chờ |
| `running` | Đang xử lý |
| `succeeded` | Hoàn thành thành công |
| `failed` | Kết thúc thất bại |
Cột `target` sẽ chỉ định worker nào xử lý công việc này. Nếu `target = 'worker_1'` thì chỉ có `worker 1` mới được phép xử lý job này, còn nếu `target = NULL` thì tất cả worker đều có thể xử lý job này.  
Cột `owner` để chúng ta biết là hiện tại worker nào đang xử lý job này, `target` và `owner` có thể khác nhau trong trường hợp `target = NULL`:  
```bash
target=NULL
owner=worker_2
```
Khi đó bất kỳ worker nào cũng có thể xử lý job này, worker nào rảnh, nhận được job này trước thì xử lý nó.  
Cột `claim_token` để hạn chế kết quả của lần chạy cũ, với mỗi lần worker nhận job, trước khi bắt đầu xử lý thì nó tạo 1 token, sau khi xử lý xong job thì chỉ được phép cập nhật dữ liệu cho token vừa tạo.  
| Lần chạy | Worker | Token |
|---:|---|---|
| 1 | worker_1 | X |
| 2 | worker_1 | Y |
Dù cùng worker, kết quả mang `token X` không được cập nhật lên lần chạy có `token Y`.  

Cột `task` chứa tác vụ, hiện tại các model chỉ cho phép chạy các tác vụ sau:  
```bash
detect  → nhận diện đối tượng
ocr     → nhận dạng văn bản
analyze → luồng phân tích kết hợp do engine triển khai
```
Cột `source` là nơi lưu trữ đường dẫn đến file cần xử lý, dựa vào đường dẫn này ta có thể lấy file từ máy chủ và xử lý.  
Cột `original_name`: tên file người dùng  
Cột `start` và `finish` để chỉ ra thời gian xử lý công việc  
| Trạng thái | started | finished |
|---|---|---|
| queued | NULL | NULL |
| running | Thời điểm nhận việc | NULL |
| succeeded | Thời điểm nhận việc | Thời điểm hoàn thành |
Cột `artifacts`: danh sách kết quả. Cột này chứa các đường dẫn tới tệp kết quả như:  
```bash
[
  "attempt_1/result.json",
  "attempt_1/annotated.jpg",
  "attempt_1/crops/object_001.jpg"
]
```
Ta ràng buộc một số trạng thái của job như sau:  
```SQL
[status] = 'queued'
AND [owner] IS NULL
AND [claim_token] IS NULL
AND [started] IS NULL
AND [finished] IS NULL
```
Ngăn trường hợp “đang chờ nhưng đã có worker sở hữu”.  

```SQL
[status] = 'running'
AND [owner] IS NOT NULL
AND [claim_token] IS NOT NULL
AND [started] IS NOT NULL
AND [finished] IS NULL
```
Ngăn trường hợp “đang chạy nhưng không biết worker nào chạy”.  

```SQL
[status] IN ('succeeded', 'failed')
AND [finished] IS NOT NULL
```
Ngăn trường hợp “đã kết thúc nhưng không có thời điểm kết thúc”.  

## Procedure
Ta có các procedure như sau:  
| Procedure | Hàm Python gọi | Nhiệm vụ |
|---|---|---|
| `Settings` | `Store.__init__()` | Đọc cấu hình queue |
| `Enqueue` | `enqueue()` | Thêm công việc |
| `Claim` | `claim()` | Worker nhận một công việc |
| `Finish` | `finish()` | Ghi kết quả thành công/thất bại |
| `GetJob` | `get()` | Lấy thông tin một công việc |
| `Running` | `running()` | Tìm công việc worker đang chạy |
| `Recover` | `recover()` | Khôi phục công việc khi worker đã dừng |
| `Expired` | `expired()` | Tìm công việc hết thời hạn lưu |
| `DeleteTerminal` | `delete_terminal()` | Xóa metadata công việc hết hạn |

Trước khi tạo procedure ta tạo các thiết lập trước  

```SQL
-- Khi tạo procedure, lưu cách xử lý NULL theo chuẩn ANSI.
SET ANSI_NULLS ON;
GO

-- Cho phép phân định tên đối tượng bằng dấu nháy kép.
-- Trong mã dưới đây vẫn dùng dấu [] cho tên đối tượng.
SET QUOTED_IDENTIFIER ON;
GO

-- Các tùy chọn phục vụ thao tác với bảng có filtered index.
-- Python cũng cần thiết lập các tùy chọn này trên connection của mình.
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET NUMERIC_ROUNDABORT OFF;
GO
```
Để đảm bảo tại 1 thời điểm chỉ được có 1 lệnh cập nhật dữ liệu thì ta sử dụng khóa chung cho ứng dụng  
```bash
Queue_job.queue.write.v1
```
Tất cả các procedure `Enqueue`, `Claim`, `Finish`, `Recover` và `DeleteTerminal` đều phải sử dụng khóa này. Khóa thuộc transaction nên được giải phóng khi commit hoặc rollback. Nó chỉ bảo vệ thao tác metadata ngắn; không giữ khóa trong lúc model xử lý file  

### Procedure Settings
Ta tạo procedure `settings` để đọc cấu hình của hàng đợi  
```SQL
-- Nếu procedure chưa tồn tại thì tạo.
-- Nếu đã tồn tại thì cập nhật định nghĩa.
CREATE OR ALTER PROCEDURE [Queue_job].[Settings]
AS
BEGIN
    -- Không gửi thông báo phụ như "(1 row affected)".
    -- SELECT bên dưới vẫn trả dữ liệu bình thường.
    SET NOCOUNT ON;

    -- Bảng cấu hình phải có dòng id=1.
    -- Nếu thiếu, báo lỗi rõ ràng thay vì trả rỗng khiến Python
    -- phát sinh lỗi khó hiểu khi truy cập kết quả [0].
    IF NOT EXISTS
    (
        SELECT 1
        FROM [Queue_job].[QueueSettings]
        WHERE [id] = 1
    )
    BEGIN
        THROW 51006, N'Không tìm thấy cấu hình hàng đợi id=1.', 1;
    END;

    -- Trả đúng các cột mà Store.__init__() sử dụng.
    SELECT
        [id],
        [max_pending],
        [max_attempts],
        [schema_version]
    FROM [Queue_job].[QueueSettings]
    WHERE [id] = 1;
END;
GO
```
Sau đó ta có thể gọi bằng cách `EXEC [Queue_job].[Settings];` và nhận được kết quả ví dụ sau:  
| id | max_pending | max_attempts | schema_version |
|---:|---:|---:|---:|
| 1 | 100 | 3 | 1 |
### procedure enqueue
Procedure này thêm công việc mới vào hàng đợi và chống gửi trùng  
Procedure này giải quyết đồng thời hai vấn đề:
- Không cho nhiều request cùng vượt giới hạn queue.
- Gửi lại cùng job ID và cùng metadata không tạo thêm job.

```SQL
CREATE OR ALTER PROCEDURE [Queue_job].[Enqueue]

    -- ID do Python tạo bằng uuid.uuid4().hex.
    @id varchar(32),

    -- Loại công việc: detect, ocr hoặc analyze.
    @task varchar(16),

    -- Đường dẫn tương đối của file đã được API lưu.
    @source nvarchar(1024),

    -- Tên file gốc để hiển thị cho người dùng.
    @name nvarchar(255),

    -- NULL: job dùng chung.
    -- Có giá trị: chỉ worker mang tên này được nhận.
    @target nvarchar(128) = NULL

AS
BEGIN
    SET NOCOUNT ON;

    -- Kết hợp TRY/CATCH để rollback khi thao tác gặp lỗi.
    SET XACT_ABORT ON;

    -- Procedure tự quản lý transaction.
    -- Không cho gọi bên trong transaction do caller mở trước.
    -- Python phải dùng autocommit=True.
    IF @@TRANCOUNT <> 0
    BEGIN
        THROW 51011, N'Caller phải sử dụng autocommit.', 1;
    END;

    BEGIN TRY

        -- Bắt đầu transaction chứa việc kiểm tra và thêm job.
        BEGIN TRANSACTION;

        -- Nhận mã kết quả khi xin khóa ứng dụng.
        DECLARE @lock_result int;

        -- Tất cả procedure ghi queue phải dùng cùng tên khóa.
        -- Exclusive: tại một thời điểm chỉ một transaction giữ khóa.
        -- Transaction: khóa tự giải phóng khi commit/rollback.
        -- 5000: chờ tối đa 5 giây.
        EXEC @lock_result = sys.sp_getapplock
            @Resource = N'Queue_job.queue.write.v1',
            @LockMode = 'Exclusive',
            @LockOwner = 'Transaction',
            @LockTimeout = 5000;

        -- Mã âm nghĩa là không lấy được khóa thành công.
        -- THROW đưa luồng vào CATCH để rollback.
        IF @lock_result < 0
        BEGIN
            THROW 51001, N'Không lấy được khóa hàng đợi.', 1;
        END;

        -- Kiểm tra ID đã có trong database hay chưa.
        IF EXISTS
        (
            SELECT 1
            FROM [Queue_job].[Jobs]
            WHERE [id] = @id
        )
        BEGIN

            -- ID đã tồn tại:
            -- chỉ chấp nhận nếu nội dung yêu cầu giống lần trước.
            --
            -- So sánh varbinary để kiểm tra chính xác nội dung
            -- đường dẫn/tên file, không phụ thuộc quy tắc so sánh
            -- không phân biệt hoa thường của database.
            IF NOT EXISTS
            (
                SELECT 1
                FROM [Queue_job].[Jobs]
                WHERE [id] = @id
                  AND [task] = @task

                  AND CONVERT(varbinary(max), [source])
                      = CONVERT(varbinary(max), @source)

                  AND CONVERT(varbinary(max), [original_name])
                      = CONVERT(varbinary(max), @name)

                  AND
                  (
                      -- NULL phải kiểm tra bằng IS NULL.
                      -- NULL = NULL không trả TRUE trong SQL.
                      ([target] IS NULL AND @target IS NULL)

                      OR

                      CONVERT(varbinary(max), [target])
                          = CONVERT(varbinary(max), @target)
                  )
            )
            BEGIN
                -- Cùng ID nhưng payload khác:
                -- không được ghi đè job đã tồn tại.
                THROW 51003, N'Job ID đã tồn tại với nội dung khác.', 1;
            END;

            -- Nếu ID và metadata giống nhau, không INSERT nữa.
            -- Request gửi lại được coi là đã được tiếp nhận.

        END
        ELSE
        BEGIN

            -- Lấy giới hạn queue từ cấu hình chung.
            DECLARE @capacity int;

            SELECT @capacity = [max_pending]
            FROM [Queue_job].[QueueSettings]
            WHERE [id] = 1;

            -- Không cho tiếp tục nếu thiếu cấu hình.
            IF @capacity IS NULL
            BEGIN
                THROW 51006, N'Không tìm thấy cấu hình hàng đợi.', 1;
            END;

            -- Đếm cả job đang chờ lẫn job đang chạy.
            -- COUNT_BIG trả về bigint.
            IF
            (
                SELECT COUNT_BIG(*)
                FROM [Queue_job].[Jobs]
                WHERE [status] IN ('queued', 'running')
            ) >= @capacity
            BEGIN
                THROW 51002, N'Hàng đợi đã đầy.', 1;
            END;

            -- Chỉ thêm job sau khi kiểm tra còn chỗ.
            -- created, attempt, artifacts dùng DEFAULT của bảng.
            -- owner, token, started, finished ban đầu là NULL.
            INSERT INTO [Queue_job].[Jobs]
            (
                [id],
                [status],
                [task],
                [source],
                [original_name],
                [target]
            )
            VALUES
            (
                @id,
                'queued',
                @task,
                @source,
                @name,
                @target
            );

        END;

        -- Ghi bền vững metadata trước khi trả xác nhận.
        COMMIT TRANSACTION;

        -- Trả ID sau commit.
        -- Đây là xác nhận đã tiếp nhận, không phải đã xử lý xong.
        SELECT @id AS [id];

    END TRY
    BEGIN CATCH

        -- XACT_STATE khác 0 nghĩa là vẫn có transaction.
        IF XACT_STATE() <> 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;

        -- Trả nguyên lỗi, giữ mã 51001/51002/51003...
        -- để Python chuyển thành exception phù hợp.
        THROW;

    END CATCH;
END;
GO
```
Ví dụ khi queue chỉ còn một chỗ:
1. Request A lấy khóa.
2. Request B phải chờ.
3. A kiểm tra còn chỗ, thêm job rồi commit.
4. B lấy khóa, đếm lại và thấy queue đầy.
5. B nhận lỗi 51002.
Nếu không đồng bộ bước đếm và bước thêm, A và B có thể cùng nhìn thấy một chỗ trống.

Lưu ý: chống trùng ở đây dựa trên job ID và metadata. Procedure không đọc file để so sánh nội dung hay hash.  

### Procedure Claim
Procedure này lấy thông tin một công việc để cho worker xử lý  
```SQL
CREATE OR ALTER PROCEDURE [Queue_job].[Claim]

    -- ID duy nhất của worker, ví dụ worker_1.
    @worker nvarchar(128),

    -- Token mới do Python tạo cho lần gọi nhận việc này.
    @token uniqueidentifier

AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @@TRANCOUNT <> 0
    BEGIN
        THROW 51011, N'Caller phải sử dụng autocommit.', 1;
    END;

    -- Không cho nhận job nếu thiếu định danh worker hoặc token.
    IF @worker IS NULL
       OR LEN(LTRIM(RTRIM(@worker))) = 0
       OR @token IS NULL
    BEGIN
        THROW 51007, N'Worker hoặc claim token không hợp lệ.', 1;
    END;

    BEGIN TRY

        BEGIN TRANSACTION;

        DECLARE @lock_result int;

        EXEC @lock_result = sys.sp_getapplock
            @Resource = N'Queue_job.queue.write.v1',
            @LockMode = 'Exclusive',
            @LockOwner = 'Transaction',
            @LockTimeout = 5000;

        IF @lock_result < 0
        BEGIN
            THROW 51001, N'Không lấy được khóa hàng đợi.', 1;
        END;

        -- Biến bảng giữ metadata của job vừa được nhận.
        -- Chỉ trả dữ liệu này về Python sau khi commit.
        DECLARE @picked TABLE
        (
            [id] varchar(32),
            [seq] bigint,
            [status] varchar(12),
            [target] nvarchar(128),
            [owner] nvarchar(128),
            [claim_token] uniqueidentifier,
            [task] varchar(16),
            [source] nvarchar(1024),
            [original_name] nvarchar(255),
            [created] datetime2(7),
            [started] datetime2(7),
            [finished] datetime2(7),
            [attempt] int,
            [error] nvarchar(2048),
            [artifacts] nvarchar(max)
        );

        -- Một worker chỉ được có một job running.
        -- Nếu đang bận thì không nhận thêm.
        IF NOT EXISTS
        (
            SELECT 1
            FROM [Queue_job].[Jobs]
            WHERE [status] = 'running'
              AND [owner] = @worker
        )
        BEGIN

            -- CTE xác định tối đa một job đủ điều kiện.
            -- Dấu ; bảo đảm câu lệnh trước WITH đã kết thúc.
            ;WITH next_job AS
            (
                SELECT TOP (1) *
                FROM [Queue_job].[Jobs] WITH (UPDLOCK, HOLDLOCK)

                -- Chỉ lấy job đang chờ.
                WHERE [status] = 'queued'

                  -- Job dùng chung hoặc dành cho đúng worker này.
                  AND
                  (
                      [target] IS NULL
                      OR [target] = @worker
                  )

                -- Ưu tiên job cũ nhất trong nhóm đủ điều kiện.
                ORDER BY [seq]
            )

            -- UPDATE qua CTE sẽ cập nhật dòng thật trong Jobs.
            UPDATE next_job
            SET
                [status] = 'running',
                [owner] = @worker,
                [claim_token] = @token,
                [started] = SYSUTCDATETIME(),
                [finished] = NULL,
                [error] = NULL,

                -- Tăng lần chạy ngay khi nhận việc.
                [attempt] = [attempt] + 1

            -- inserted là dữ liệu SAU UPDATE.
            -- Không có nghĩa tạo một job khác.
            OUTPUT
                inserted.[id],
                inserted.[seq],
                inserted.[status],
                inserted.[target],
                inserted.[owner],
                inserted.[claim_token],
                inserted.[task],
                inserted.[source],
                inserted.[original_name],
                inserted.[created],
                inserted.[started],
                inserted.[finished],
                inserted.[attempt],
                inserted.[error],
                inserted.[artifacts]

            -- Giữ kết quả nội bộ; chưa gửi cho client ở bước này.
            INTO @picked;

        END;

        -- Chốt quyền sở hữu job trước khi worker bắt đầu inference.
        COMMIT TRANSACTION;

        -- Có job: trả một dòng.
        -- Không có job phù hợp hoặc worker đang bận: trả rỗng.
        SELECT *
        FROM @picked;

    END TRY
    BEGIN CATCH

        IF XACT_STATE() <> 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;

        THROW;

    END CATCH;
END;
GO
```
Ví dụ:  
| seq | Job | target |
|---:|---|---|
| 10 | A | worker_2 |
| 11 | B | NULL |
| 12 | C | worker_1 |

Khi `worker_1` gọi `Claim`, nó nhận B:
- A chỉ dành cho worker_2.
- B dùng chung và đến trước C.
`UPDLOCK` dùng khóa cập nhật khi đọc; `HOLDLOCK` giữ ngữ nghĩa khóa serializable cho tham chiếu bảng trong transaction. Không nên hiểu rằng các hint này luôn chỉ khóa đúng một dòng; phạm vi khóa thực tế phụ thuộc kế hoạch thực thi.

Sau khi procedure trả kết quả, Python mới đọc file và gọi model. Lúc đó transaction nhận việc đã kết thúc.  

### Procedure Finish
Procedure này ghi kết quả và chống kết quả cũ ghi đè.  
```SQL
CREATE OR ALTER PROCEDURE [Queue_job].[Finish]

    -- Job cần hoàn tất.
    @id varchar(32),

    -- Worker báo kết quả.
    @owner nvarchar(128),

    -- Lần chạy mà kết quả này thuộc về.
    @attempt int,

    -- Token được cấp khi nhận job.
    @token uniqueidentifier,

    -- JSON chứa các đường dẫn kết quả đã được publish.
    @artifacts nvarchar(max),

    -- NULL nghĩa là thành công.
    -- Có nội dung nghĩa là thất bại.
    @error nvarchar(2048) = NULL

AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @@TRANCOUNT <> 0
    BEGIN
        THROW 51011, N'Caller phải sử dụng autocommit.', 1;
    END;

    -- Kiểm tra NULL riêng vì ISJSON(NULL) trả NULL.
    -- JSON hợp lệ chưa có nghĩa file thực tế đã tồn tại:
    -- việc ghi/publish file thuộc trách nhiệm Python.
    IF @artifacts IS NULL OR ISJSON(@artifacts) <> 1
    BEGIN
        THROW 51004, N'Artifacts phải là JSON hợp lệ.', 1;
    END;

    BEGIN TRY

        BEGIN TRANSACTION;

        DECLARE @lock_result int;

        EXEC @lock_result = sys.sp_getapplock
            @Resource = N'Queue_job.queue.write.v1',
            @LockMode = 'Exclusive',
            @LockOwner = 'Transaction',
            @LockTimeout = 5000;

        IF @lock_result < 0
        BEGIN
            THROW 51001, N'Không lấy được khóa hàng đợi.', 1;
        END;

        -- Suy ra trạng thái kết thúc từ error.
        -- Chuỗi rỗng '' vẫn khác NULL và sẽ được coi là failed.
        DECLARE @state varchar(12) =
            CASE
                WHEN @error IS NULL THEN 'succeeded'
                ELSE 'failed'
            END;

        -- Mặc định chưa chấp nhận kết quả.
        DECLARE @ok bit = 0;

        -- Chỉ cập nhật nếu kết quả thuộc đúng lần chạy đang hoạt động.
        UPDATE [Queue_job].[Jobs]
        SET
            [status] = @state,
            [finished] = SYSUTCDATETIME(),
            [error] = @error,
            [artifacts] = @artifacts
        WHERE [id] = @id
          AND [owner] = @owner
          AND [attempt] = @attempt
          AND [claim_token] = @token
          AND [status] = 'running';

        -- Phải đọc @@ROWCOUNT ngay sau UPDATE.
        -- Một dòng được sửa nghĩa là kết quả đã được chấp nhận.
        IF @@ROWCOUNT = 1
        BEGIN
            SET @ok = 1;
        END
        ELSE IF EXISTS
        (
            -- Hỗ trợ gửi lại đúng kết quả đã commit trước đó.
            --
            -- Ví dụ: SQL commit thành công nhưng mạng mất,
            -- worker chưa nhận được phản hồi và gửi lại.
            SELECT 1
            FROM [Queue_job].[Jobs]
            WHERE [id] = @id
              AND [owner] = @owner
              AND [attempt] = @attempt
              AND [claim_token] = @token
              AND [status] = @state

              -- So sánh chính xác chuỗi JSON đã lưu.
              AND CONVERT(varbinary(max), [artifacts])
                  = CONVERT(varbinary(max), @artifacts)

              AND
              (
                  ([error] IS NULL AND @error IS NULL)

                  OR

                  CONVERT(varbinary(max), [error])
                      = CONVERT(varbinary(max), @error)
              )
        )
        BEGIN
            -- Kết quả trùng khớp đã được ghi trước đó.
            SET @ok = 1;
        END;

        COMMIT TRANSACTION;

        -- accepted=1: kết quả được chấp nhận.
        -- accepted=0: job không thuộc lần chạy này hoặc payload khác.
        SELECT @ok AS [accepted];

    END TRY
    BEGIN CATCH

        IF XACT_STATE() <> 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;

        THROW;

    END CATCH;
END;
GO
```
Ví dụ  
| Tình huống | attempt | token |
|---|---:|---|
| Lần chạy ban đầu | 1 | X |
| Sau recovery và nhận lại | 2 | Y |
| Kết quả cũ gửi về | 1 | X |

Kết quả cũ không thỏa điều kiện `WHERE`, nên không ghi đè kết quả lần chạy mới.
`accepted=1` không đồng nghĩa job thành công. Nếu worker báo lỗi hợp lệ, trạng thái `failed` vẫn được chấp nhận.
Việc so sánh `artifacts` là so sánh chuỗi JSON chính xác. Hai JSON tương đương về ý nghĩa nhưng khác khoảng trắng hoặc thứ tự phần tử có thể không được coi là cùng payload.  

### Procedure Getjob
Procedure này lấy thông tin của 1 job  
```SQL
CREATE OR ALTER PROCEDURE [Queue_job].[GetJob]

    -- ID lấy từ URL /jobs/{job_id}.
    @id varchar(32)

AS
BEGIN
    SET NOCOUNT ON;

    -- Liệt kê rõ các cột để hợp đồng dữ liệu dễ kiểm soát.
    -- Không có dòng phù hợp thì trả tập kết quả rỗng.
    SELECT
        [seq],
        [id],
        [status],
        [target],
        [owner],
        [claim_token],
        [task],
        [source],
        [original_name],
        [created],
        [started],
        [finished],
        [attempt],
        [error],
        [artifacts]
    FROM [Queue_job].[Jobs]
    WHERE [id] = @id;
END;
GO
```
Nếu không có job, Python trả None, rồi API trả HTTP 404.  
Procedure trả metadata cho nội bộ ứng dụng. API vẫn cần chọn các trường public và kiểm tra quyền truy cập của người dùng trước khi trả dữ liệu.  

### Procedure running
Procedure này tìm 1 job mà worker đang xử lý  
```SQL
CREATE OR ALTER PROCEDURE [Queue_job].[Running]

    -- Worker cần kiểm tra.
    @worker nvarchar(128)

AS
BEGIN
    SET NOCOUNT ON;

    -- Supervisor chỉ cần ID job và thời điểm bắt đầu
    -- để theo dõi thời gian xử lý.
    SELECT
        [id],
        [started]
    FROM [Queue_job].[Jobs]
    WHERE [status] = 'running'
      AND [owner] = @worker;

    -- Unique filtered index trên owner khi status='running'
    -- bảo đảm tối đa một dòng cho mỗi worker.
END;
GO
```
Nếu worker đang rảnh, kết quả rỗng. Nếu đang chạy thì sẽ có thông tin, ta có thể dựa vào thời gian chạy (quá lâu) để biết nó có bị treo hay không, ...  

### Procedure recovery
Procedure này khôi phục job của 1 worker đã dừng. Chỉ gọi sau khi supervisor đã xác nhận tiến trình cũ kết thúc. Cờ `@confirmed_stopped` là xác nhận từ caller; SQL Server không dùng nó để kiểm tra PID thật.  
```SQL
CREATE OR ALTER PROCEDURE [Queue_job].[Recover]

    -- Có giá trị: khôi phục job của một worker.
    -- NULL: khôi phục tất cả job đang running.
    @worker nvarchar(128) = NULL,

    -- Mặc định không cho recovery nếu caller chưa xác nhận.
    @confirmed_stopped bit = 0

AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @@TRANCOUNT <> 0
    BEGIN
        THROW 51011, N'Caller phải sử dụng autocommit.', 1;
    END;

    -- Kiểm tra cả NULL để không bỏ lọt giá trị không xác định.
    IF @confirmed_stopped IS NULL OR @confirmed_stopped <> 1
    BEGIN
        THROW 51005, N'Phải xác nhận tiến trình cũ đã dừng trước khi recovery.', 1;
    END;

    BEGIN TRY

        BEGIN TRANSACTION;

        DECLARE @lock_result int;

        EXEC @lock_result = sys.sp_getapplock
            @Resource = N'Queue_job.queue.write.v1',
            @LockMode = 'Exclusive',
            @LockOwner = 'Transaction',
            @LockTimeout = 5000;

        IF @lock_result < 0
        BEGIN
            THROW 51001, N'Không lấy được khóa hàng đợi.', 1;
        END;

        -- Đọc số lần nhận chạy tối đa.
        DECLARE @maximum int;

        SELECT @maximum = [max_attempts]
        FROM [Queue_job].[QueueSettings]
        WHERE [id] = 1;

        IF @maximum IS NULL
        BEGIN
            THROW 51006, N'Không tìm thấy cấu hình hàng đợi.', 1;
        END;

        -- Giữ danh sách job đã bị thay đổi để trả về ghi log.
        DECLARE @changed TABLE
        (
            [id] varchar(32)
        );

        UPDATE [Queue_job].[Jobs]
        SET

            -- Còn lượt thì quay lại queue.
            -- Hết lượt thì kết thúc thất bại.
            [status] =
                CASE
                    WHEN [attempt] < @maximum THEN 'queued'
                    ELSE 'failed'
                END,

            -- Thu hồi quyền sở hữu của lần chạy cũ.
            [owner] = NULL,
            [claim_token] = NULL,

            -- Job quay lại queue sẽ có started mới khi Claim.
            [started] = NULL,

            -- Chỉ ghi thời điểm kết thúc khi không thử lại nữa.
            [finished] =
                CASE
                    WHEN [attempt] < @maximum THEN NULL
                    ELSE SYSUTCDATETIME()
                END,

            -- Lưu lý do thay đổi để phục vụ theo dõi.
            [error] =
                CASE
                    WHEN [attempt] < @maximum
                        THEN N'Worker stopped; retry scheduled'
                    ELSE N'Worker stopped; retry limit reached'
                END

        -- Thu thập ID các dòng vừa thay đổi.
        OUTPUT inserted.[id]
        INTO @changed

        -- Chỉ khôi phục job đang chạy.
        WHERE [status] = 'running'

          -- NULL nghĩa là toàn bộ worker.
          -- Có giá trị nghĩa là chỉ worker được chỉ định.
          AND
          (
              @worker IS NULL
              OR [owner] = @worker
          );

        -- Không reset attempt:
        -- Claim lần sau sẽ tăng từ giá trị đang có.
        --
        -- Không thay target:
        -- job dành riêng cho worker_1 vẫn dành cho worker_1.

        COMMIT TRANSACTION;

        -- Trả cả ID được requeue lẫn ID bị chuyển sang failed.
        SELECT [id]
        FROM @changed;

    END TRY
    BEGIN CATCH

        IF XACT_STATE() <> 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;

        THROW;

    END CATCH;
END;
GO
```
Ví dụ khi ta có `max_attempts = 3`  
| attempt trước recovery | Trạng thái sau recovery |
|---:|---|
| 1 | queued |
| 2 | queued |
| 3 | failed |

### Procedure Expire 
Procedure này tìm các job đã hết thời gian lưu  
```SQL
CREATE OR ALTER PROCEDURE [Queue_job].[Expired]

    -- Mốc thời gian UTC.
    -- Các job kết thúc trước mốc này được coi là hết hạn.
    @cutoff datetime2(7)

AS
BEGIN
    SET NOCOUNT ON;

    -- Thiếu cutoff có thể che giấu lỗi cấu hình cleanup.
    IF @cutoff IS NULL
    BEGIN
        THROW 51007, N'Phải cung cấp thời điểm cutoff.', 1;
    END;

    -- Chỉ liệt kê job đã kết thúc.
    -- Không trả job queued/running để tránh dọn file đang cần dùng.
    SELECT
        [id],
        [status],
        [finished]
    FROM [Queue_job].[Jobs]
    WHERE [status] IN ('succeeded', 'failed')
      AND [finished] < @cutoff

    -- Dọn các job cũ trước.
    ORDER BY [finished], [seq];
END;
GO
```
### Procedure DeleteTerminal
Procedure này xóa các metadata của job đã kết thúc và nó đã hết hạn  
```SQL
CREATE OR ALTER PROCEDURE [Queue_job].[DeleteTerminal]

    -- Job cần xóa.
    @id varchar(32),

    -- Chỉ xóa nếu job kết thúc trước mốc UTC này.
    @cutoff datetime2(7)

AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @@TRANCOUNT <> 0
    BEGIN
        THROW 51011, N'Caller phải sử dụng autocommit.', 1;
    END;

    IF @cutoff IS NULL
    BEGIN
        THROW 51007, N'Phải cung cấp thời điểm cutoff.', 1;
    END;

    BEGIN TRY

        BEGIN TRANSACTION;

        DECLARE @lock_result int;

        EXEC @lock_result = sys.sp_getapplock
            @Resource = N'Queue_job.queue.write.v1',
            @LockMode = 'Exclusive',
            @LockOwner = 'Transaction',
            @LockTimeout = 5000;

        IF @lock_result < 0
        BEGIN
            THROW 51001, N'Không lấy được khóa hàng đợi.', 1;
        END;

        -- Kiểm tra lại điều kiện tại thời điểm xóa.
        -- Không chỉ tin vào kết quả Expired lấy trước đó.
        DELETE FROM [Queue_job].[Jobs]
        WHERE [id] = @id
          AND [status] IN ('succeeded', 'failed')
          AND [finished] < @cutoff;

        -- Ghi số dòng bị xóa ngay sau DELETE.
        DECLARE @deleted int = @@ROWCOUNT;

        COMMIT TRANSACTION;

        -- 1: đã xóa đúng một dòng.
        -- 0: không còn dòng hoặc không đáp ứng điều kiện.
        SELECT @deleted AS [deleted];

    END TRY
    BEGIN CATCH

        IF XACT_STATE() <> 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;

        THROW;

    END CATCH;
END;
GO
```
Procedure này chỉ xóa metadata trong SQL Server. Nó không xóa ảnh gốc, PDF, bounding box, text hay crop trên storage.  
SQL transaction không tự bao phủ thao tác filesystem của Python. Cleanup cần có cơ chế xử lý lại khi xóa file hoặc xóa metadata bị gián đoạn; không nên coi hai thao tác đó là một transaction duy nhất.  

# Tạo Service nhận job