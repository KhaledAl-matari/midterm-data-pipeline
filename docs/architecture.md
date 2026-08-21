# معمارية نظام Hybrid Big Data ELT Pipeline

## 1. نظرة عامة

يعتمد النظام على بنية ELT هجينة لمعالجة ملفات CSV التي تحتوي على بيانات طلبات غير نظيفة.

الهدف الأساسي هو اختيار محرك المعالجة المناسب تلقائيًا حسب حجم الملف، ثم تحميل البيانات الخام إلى MongoDB قبل تنفيذ أي عملية تنظيف أو تحقق.

---

## 2. تدفق البيانات

```text
                CSV Input
                    |
                    v
              File Router
                    |
          +---------+---------+
          |                   |
          v                   v
   Python Batch            PySpark
   <= 200 MB               > 200 MB
          |                   |
          +---------+---------+
                    |
                    v
               orders_raw
                    |
                    v
          Cleaning + Validation
                    |
          +---------+---------+
          |                   |
          v                   v
 orders_validated      orders_quarantine
          |
          v
   Idempotent Upsert

                    |
                    v
          Metrics & Reports
                    |
                    v
         reports/results.json
3. File Router

يقوم File Router بفحص حجم ملف الإدخال قبل بدء المعالجة.

الحد المستخدم:

200 MB

آلية الاختيار:

File Size <= 200 MB  -> Python Batch
File Size > 200 MB   -> PySpark

ويتم تسجيل:

اسم الملف.
حجم الملف.
المحرك المختار.
سبب الاختيار.
4. مسار Python Batch

يستخدم للملفات الصغيرة.

آلية التنفيذ:

CSV
 |
 v
Streaming Reader
 |
 v
Batch = 5000 Records
 |
 v
insert_many()
 |
 v
orders_raw

المميزات:

لا يتم تحميل الملف كاملًا في الذاكرة.
القراءة تتم تدريجيًا.
الكتابة إلى MongoDB تتم على دفعات.
يتم تسجيل الزمن وThroughput.
5. مسار PySpark

يستخدم للملفات الكبيرة.

آلية التنفيذ:

Large CSV
   |
   v
Spark DataFrame
   |
   v
Fixed Schema
   |
   v
Partitions
   |
   v
MongoDB Spark Connector
   |
   v
orders_raw

المميزات:

Parallel Processing.
DataFrame API.
Fixed Schema.
عدم استخدام Pandas لمعالجة الملف الكبير.
عدم استخدام inferSchema.
تنفيذ Partition-based Processing.

في التشغيل الكبير تم استخدام:

30,000,000 Rows
99 Partitions
6. Raw Layer

يتم إدخال جميع السجلات أولًا إلى:

orders_raw

قبل أي Cleaning أو Validation.

يحتوي Raw على معلومات مثل:

id_run
file_source
engine_used
at_ingested
record_raw
processing_outcome

يتم الحفاظ على:

record_raw

دون تعديل.

7. Cleaning & Validation

بعد اكتمال Raw تبدأ مرحلة معالجة البيانات.

يتم تطبيق قواعد محددة مثل:

Trim Whitespace.
Required Business Keys.
Date Normalization.
Money Normalization.
Currency Normalization.
Phone Normalization.
Email Correction.
JSON Validation.
Quantity Validation.
Total Recalculation.

كل تصحيح يتم تسجيله داخل Audit Trail.

8. تصنيف السجلات

بعد تطبيق القواعد يحصل كل Raw Record على Processing Outcome واحد فقط.

Valid / Corrected

يتم إرساله إلى:

orders_validated
غير قابل للتصحيح بأمان

يتم إرساله إلى:

orders_quarantine

ولا يتم حذف البيانات السيئة.

9. orders_validated

يحتوي على البيانات السليمة والمصححة.

يتم استخدام:

order_id

كمفتاح Business Key ثابت.

ويطبق:

Unique Index
+
Upsert

لمنع Duplicate Business Records.

10. orders_quarantine

يحتوي على السجلات التي لا يمكن تصحيحها بأمان.

يتم الاحتفاظ بـ:

error_codes
error_details
corrections_attempted
raw record

وذلك لضمان عدم فقد البيانات وإمكانية تحليل الأخطاء لاحقًا.

11. Idempotency

إعادة معالجة نفس البيانات لا تؤدي إلى إنشاء سجلات أعمال مكررة.

المنطق:

New order_id      -> Insert
Existing order_id -> Update
Same final value  -> Unchanged

ويتم ضمان ذلك باستخدام Unique Index على:

order_id
12. قاعدة الاتساق

لكل Run يجب أن تتحقق المعادلة:

run_raw_count
=
run_valid_count
+
run_corrected_count
+
run_quarantine_count

في التشغيل الكبير:

30,000,000
=
0
+
27,271,346
+
2,728,654

النتيجة:

PASS
13. Metrics Layer

يتم تسجيل نتائج التشغيل في:

reports/results.json

وتشمل:

run_id
file_name
file_size_mb
engine_used
rows_read
raw_loaded
valid_count
corrected_count
quarantine_count
elapsed_seconds
throughput
batch_size
partitions
error_case_counts
inserted_count
updated_count
unchanged_count
14. المكونات الرئيسية
src/main.py
    |
    +-- file_router.py
    |
    +-- batch_loader.py
    |
    +-- spark_loader.py
    |
    +-- elt_pipeline.py
    |
    +-- quality_rules.py
    |
    +-- mongo_setup.py
    |
    `-- metrics.py
15. MongoDB

قاعدة البيانات المستخدمة:

midterm_data_pipeline

والـCollections الرئيسية:

orders_raw
orders_validated
orders_quarantine

يتم تطبيق Schema Validation على:

orders_validated

ويتم تطبيق Unique Index على:

order_id
16. مبدأ التصميم

المعمارية تعتمد على المبادئ التالية:

Raw First.
No Silent Data Loss.
Deterministic Cleaning.
Safe Correction Only.
Quarantine Instead of Deletion.
Stable Business Key.
Idempotent Upsert.
Automatic Engine Selection.
Measurable Performance.
Reproducible Processing.
