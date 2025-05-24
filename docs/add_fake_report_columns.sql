-- Add columns for fake reporting
ALTER TABLE public.closed_trades ADD COLUMN IF NOT EXISTS fake_volume numeric DEFAULT 0;
ALTER TABLE public.closed_trades ADD COLUMN IF NOT EXISTS fake_pnl numeric DEFAULT 0;
