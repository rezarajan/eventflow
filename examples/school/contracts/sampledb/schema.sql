-- Synthetic operational source database contract.
-- This schema models a fake national school operational system for PoC data generation.

CREATE SCHEMA IF NOT EXISTS sampledb;

\i tables/schools.sql
\i tables/classes.sql
\i tables/students.sql
\i tables/subjects.sql
\i tables/attendance_records.sql
\i tables/exam_papers.sql
\i tables/grades.sql
\i tables/document_uploads.sql
\i tables/audit_events.sql
