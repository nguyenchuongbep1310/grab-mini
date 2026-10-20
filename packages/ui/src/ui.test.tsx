import "@testing-library/jest-dom/vitest";
import { render, screen, fireEvent, act } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import {
  Avatar,
  BottomSheet,
  Button,
  CancelDialog,
  Card,
  PriceTag,
  SegmentedControl,
  StarRating,
  Stat,
  StatusBadge,
  SurgeBadge,
  ToastProvider,
  TripStatusTimeline,
  reasonFor,
  useToast,
  MAX_REASON_LENGTH,
} from "./index";

describe("Button", () => {
  it("renders with variant, size, block, and handles clicks", () => {
    const onClick = vi.fn();
    render(
      <Button variant="danger" size="lg" block onClick={onClick}>
        Delete
      </Button>,
    );
    const btn = screen.getByRole("button", { name: "Delete" });
    expect(btn).toHaveClass("gm-btn", "gm-btn--danger", "gm-btn--lg", "gm-btn--block");
    expect(btn).not.toBeDisabled();
    fireEvent.click(btn);
    expect(onClick).toHaveBeenCalledTimes(1);
  });

  it("disables and shows spinner when loading", () => {
    const onClick = vi.fn();
    render(
      <Button loading onClick={onClick}>
        Saving
      </Button>,
    );
    const btn = screen.getByRole("button", { name: "Saving" });
    expect(btn).toBeDisabled();
    expect(btn).toHaveAttribute("aria-busy", "true");
    expect(btn.querySelector(".gm-spinner")).not.toBeNull();
  });
});

describe("StatusBadge & SurgeBadge & PriceTag", () => {
  it("renders known status styles and supports custom label override", () => {
    const { rerender } = render(<StatusBadge status="PENDING" />);
    expect(screen.getByText("Finding driver")).toHaveClass("gm-badge--warning", "gm-badge--pulse");

    rerender(<StatusBadge status="ASSIGNED" label="Heading to pickup" />);
    expect(screen.getByText("Heading to pickup")).toHaveClass("gm-badge--info");

    rerender(<StatusBadge status="CUSTOM_STATUS" />);
    expect(screen.getByText("CUSTOM_STATUS")).toHaveClass("gm-badge--neutral");
  });

  it("renders SurgeBadge only when multiplier > 1.0", () => {
    const { container, rerender } = render(<SurgeBadge multiplier={1.0} />);
    expect(container).toBeEmptyDOMElement();

    rerender(<SurgeBadge multiplier={1.4} />);
    expect(screen.getByRole("status")).toHaveTextContent("x1.4 high demand");
  });

  it("formats PriceTag in VND and renders struck-through original fare when changed", () => {
    const { container, rerender } = render(<PriceTag amount={45000} />);
    expect(container.querySelector(".gm-price__amount")?.textContent).toMatch(/45\.000/);
    expect(container.querySelector(".gm-price__was")).toBeNull();

    rerender(<PriceTag amount={63000} was={45000} size="lg" />);
    expect(container.querySelector(".gm-price__was")?.textContent).toMatch(/45\.000/);
    expect(container.querySelector(".gm-price__amount")?.textContent).toMatch(/63\.000/);
  });
});

describe("SegmentedControl", () => {
  const options = [
    { value: "CAR" as const, label: "Car", hint: "Up to 4 riders" },
    { value: "BIKE" as const, label: "Bike", hint: "Quick and cheap" },
  ];

  it("renders options, selects on click, and navigates with arrow keys", () => {
    const onChange = vi.fn();
    render(<SegmentedControl label="Vehicle type" options={options} value="CAR" onChange={onChange} />);

    const carRadio = screen.getByRole("radio", { name: /Car/ });
    const bikeRadio = screen.getByRole("radio", { name: /Bike/ });
    expect(carRadio).toHaveAttribute("aria-checked", "true");
    expect(carRadio).toHaveAttribute("tabIndex", "0");
    expect(bikeRadio).toHaveAttribute("aria-checked", "false");
    expect(bikeRadio).toHaveAttribute("tabIndex", "-1");

    fireEvent.click(bikeRadio);
    expect(onChange).toHaveBeenCalledWith("BIKE");

    fireEvent.keyDown(carRadio, { key: "ArrowRight" });
    expect(onChange).toHaveBeenLastCalledWith("BIKE");

    fireEvent.keyDown(carRadio, { key: "ArrowLeft" });
    expect(onChange).toHaveBeenLastCalledWith("BIKE");
  });
});

describe("StarRating", () => {
  it("renders read-only mode with accessible label and percentage fill", () => {
    render(<StarRating value={4.5} label="Driver rating" />);
    const img = screen.getByRole("img", { name: "Driver rating: 4.5 out of 5 stars" });
    const fill = img.querySelector(".gm-stars__fill") as HTMLElement;
    expect(fill.style.width).toBe("90%");
  });

  it("renders interactive radio group, handles click and keyboard arrows", () => {
    const onChange = vi.fn();
    render(<StarRating value={3} onChange={onChange} label="Rate your driver" />);

    const star4 = screen.getByRole("radio", { name: "4 stars" });
    fireEvent.click(star4);
    expect(onChange).toHaveBeenCalledWith(4);

    const star3 = screen.getByRole("radio", { name: "3 stars" });
    fireEvent.keyDown(star3, { key: "ArrowRight" });
    expect(onChange).toHaveBeenLastCalledWith(4);

    fireEvent.keyDown(star3, { key: "ArrowLeft" });
    expect(onChange).toHaveBeenLastCalledWith(2);
  });
});

describe("TripStatusTimeline", () => {
  const steps = [
    { key: "searching", label: "Finding a driver" },
    { key: "assigned", label: "Driver on the way", hint: "2 mins away" },
    { key: "in_progress", label: "On your trip" },
    { key: "completed", label: "Arrived" },
  ];

  it("marks steps before currentKey as done, currentKey as current, and later as upcoming", () => {
    const { container, rerender } = render(<TripStatusTimeline steps={steps} currentKey="assigned" />);
    const items = container.querySelectorAll(".gm-timeline__step");
    expect(items[0]).toHaveClass("gm-timeline__step--done");
    expect(items[1]).toHaveClass("gm-timeline__step--current");
    expect(items[1]).toHaveAttribute("aria-current", "step");
    expect(screen.getByText("2 mins away")).toBeInTheDocument();
    expect(items[2]).toHaveClass("gm-timeline__step--upcoming");

    // Failed state marks current step as failed
    rerender(<TripStatusTimeline steps={steps} currentKey="assigned" failed />);
    expect(container.querySelectorAll(".gm-timeline__step")[1]).toHaveClass("gm-timeline__step--failed");

    // Reaching last step marks it as done
    rerender(<TripStatusTimeline steps={steps} currentKey="completed" />);
    expect(container.querySelectorAll(".gm-timeline__step")[3]).toHaveClass("gm-timeline__step--done");
  });
});

describe("CancelDialog & reasonFor", () => {
  it("normalizes and truncates reasons via reasonFor", () => {
    expect(reasonFor("Waited too long", "ignored")).toBe("Waited too long");
    expect(reasonFor("__other__", "  custom reason  ")).toBe("custom reason");
    expect(reasonFor("__other__", "x".repeat(250))).toHaveLength(MAX_REASON_LENGTH);
  });

  it("lets user pick a preset or custom reason and confirms or dismisses", () => {
    const onConfirm = vi.fn();
    const onDismiss = vi.fn();
    render(
      <CancelDialog
        open
        title="Cancel this ride?"
        description="We will stop searching."
        reasons={["Changed my plans", "Waited too long"]}
        onConfirm={onConfirm}
        onDismiss={onDismiss}
      />,
    );

    expect(screen.getByRole("dialog", { name: "Cancel this ride?" })).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText("Waited too long"));
    fireEvent.click(screen.getByRole("button", { name: "Cancel ride" }));
    expect(onConfirm).toHaveBeenCalledWith("Waited too long");

    // Custom "Other" reason
    fireEvent.click(screen.getByLabelText("Other"));
    const textarea = screen.getByPlaceholderText("Tell us more");
    fireEvent.change(textarea, { target: { value: "Driver went wrong way" } });
    fireEvent.click(screen.getByRole("button", { name: "Cancel ride" }));
    expect(onConfirm).toHaveBeenLastCalledWith("Driver went wrong way");

    // Escape key dismisses
    fireEvent.keyDown(document, { key: "Escape" });
    expect(onDismiss).toHaveBeenCalledTimes(1);
  });
});

describe("ToastProvider & useToast", () => {
  function Demo() {
    const toast = useToast();
    return <button onClick={() => toast.show("Ride booked!", "success")}>Trigger</button>;
  }

  it("shows toast messages and auto-dismisses after durationMs", () => {
    vi.useFakeTimers();
    try {
      render(
        <ToastProvider durationMs={2000}>
          <Demo />
        </ToastProvider>,
      );
      fireEvent.click(screen.getByRole("button", { name: "Trigger" }));
      expect(screen.getByRole("status")).toHaveTextContent("Ride booked!");

      act(() => {
        vi.advanceTimersByTime(2000);
      });
      expect(screen.queryByRole("status")).toBeNull();
    } finally {
      vi.useRealTimers();
    }
  });
});

describe("Avatar, Card, BottomSheet, Stat", () => {
  it("renders initials in Avatar and renders Card/BottomSheet/Stat", () => {
    render(
      <BottomSheet>
        <Card padding="sm">
          <Avatar name="driver-2" />
          <Stat label="Match rate" value="98%" hint="last hour" tone="success" />
        </Card>
      </BottomSheet>,
    );
    expect(screen.getByText("Match rate")).toBeInTheDocument();
    expect(screen.getByText("98%")).toBeInTheDocument();
  });
});
