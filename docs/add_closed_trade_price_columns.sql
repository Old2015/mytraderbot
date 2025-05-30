-- Add columns for detailed closed trade info
ALTER TABLE public.closed_trades ADD COLUMN IF NOT EXISTS entry_price numeric DEFAULT 0;
ALTER TABLE public.closed_trades ADD COLUMN IF NOT EXISTS exit_price numeric DEFAULT 0;
ALTER TABLE public.closed_trades ADD COLUMN IF NOT EXISTS stop_price numeric DEFAULT 0;
ALTER TABLE public.closed_trades ADD COLUMN IF NOT EXISTS take_price numeric DEFAULT 0;
ALTER TABLE public.closed_trades ADD COLUMN IF NOT EXISTS reason text DEFAULT 'market';
ALTER TABLE public.closed_trades ADD COLUMN IF NOT EXISTS rr numeric DEFAULT 0;
