-- Add parent_run_id column to runs table for parent-child run linkage.
-- This allows tracking when kilroy is invoked from inside another kilroy run's stage.
-- No FK constraint: children outlive parents in the filesystem.
ALTER TABLE runs ADD COLUMN parent_run_id TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_runs_parent_run_id ON runs(parent_run_id);
