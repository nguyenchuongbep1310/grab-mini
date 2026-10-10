import { cx } from "./cx";

export interface PriceTagProps {
  /** Amount in the currency's main unit (VND has no minor unit). */
  amount: number;
  currency?: string;
  /** Size of the figure. */
  size?: "md" | "lg";
  /** Shown struck through before the amount, e.g. the fare before surge. */
  was?: number;
  className?: string;
}

const formatters = new Map<string, Intl.NumberFormat>();

function format(amount: number, currency: string): string {
  let f = formatters.get(currency);
  if (!f) {
    f = new Intl.NumberFormat("vi-VN", { style: "currency", currency, maximumFractionDigits: 0 });
    formatters.set(currency, f);
  }
  return f.format(amount);
}

// A price, formatted the same way everywhere (rider sheet, receipts, ops).
export function PriceTag({ amount, currency = "VND", size = "md", was, className }: PriceTagProps) {
  return (
    <span className={cx("gm-price", `gm-price--${size}`, className)}>
      {was !== undefined && was !== amount && <s className="gm-price__was">{format(was, currency)}</s>}
      <span className="gm-price__amount">{format(amount, currency)}</span>
    </span>
  );
}
