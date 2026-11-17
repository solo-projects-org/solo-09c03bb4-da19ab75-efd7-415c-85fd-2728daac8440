-- 复测核定 schema（与 01_schema.sql 同库；API 启动时也会幂等执行，保证老库可升级）。
--
-- 设计要点：
-- 1. 核定值与两次原始读数、版本号同库同事务写入；
-- 2. (样品行 ID, 版本) 唯一：同版本同内容重试可识别为同一单、内容冲突可拒绝；
-- 3. 不带外键约束：源库直接 DELETE 样品不得被业务表阻止，删后也能据此识别“核定已失效”；
-- 4. 样品行上的行版本 xmin 快照用于识别“源库直接改值”——改值后旧核定不再算现行。

-- 样品行上的现行核定结果（与原始 ph 并列，便于 PITR 一同恢复、一同出现）
ALTER TABLE public.samples
    ADD COLUMN IF NOT EXISTS retest_version     integer,
    ADD COLUMN IF NOT EXISTS ph_reading_a       numeric(4,2),
    ADD COLUMN IF NOT EXISTS ph_reading_b       numeric(4,2),
    ADD COLUMN IF NOT EXISTS approved_ph        numeric(4,2),
    ADD COLUMN IF NOT EXISTS retest_row_xmin    xid,   -- 核定时样品行的 xmin 快照
    ADD COLUMN IF NOT EXISTS approved_retest_id bigint;

-- 复测核定登记：每次提交（合格 / 超差待复核）都留痕，版本只增不减
CREATE TABLE IF NOT EXISTS public.ph_retests (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sample_no       text NOT NULL,                          -- 请求带来的样品号（冗余留痕）
    sample_row_id   bigint NOT NULL,                        -- 请求针对的样品行 ID
    version         integer NOT NULL,
    ph_reading_a    numeric(4,2) NOT NULL CHECK (ph_reading_a BETWEEN 0 AND 14),
    ph_reading_b    numeric(4,2) NOT NULL CHECK (ph_reading_b BETWEEN 0 AND 14),
    diff            numeric(4,2) NOT NULL,
    approved_ph     numeric(4,2),                           -- 超差时为 NULL
    status          text NOT NULL CHECK (status IN
                        ('approved', 'pending_review', 'superseded', 'obsolete')),
    conclusion      text NOT NULL,
    idempotency_key text NOT NULL,                          -- 同版本同内容重试的指纹
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (sample_row_id, version)
);

-- 幂等指纹也唯一：同样品行上同内容（跨版本）也能快速定位
CREATE UNIQUE INDEX IF NOT EXISTS uq_ph_retests_idem
    ON public.ph_retests (sample_row_id, idempotency_key);

CREATE INDEX IF NOT EXISTS idx_ph_retests_sample
    ON public.ph_retests (sample_row_id, version DESC);
