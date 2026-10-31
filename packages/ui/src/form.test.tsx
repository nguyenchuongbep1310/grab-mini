import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { FormSheet, SelectField, TextField } from "./index";

describe("TextField", () => {
  it("labels the input and links a hint to it", () => {
    render(<TextField label="Name" hint="As on your ID" defaultValue="An" />);
    const input = screen.getByLabelText("Name");
    expect(input).toHaveValue("An");
    expect(input).toHaveAccessibleDescription("As on your ID");
    expect(input).not.toHaveAttribute("aria-invalid");
  });

  it("marks the input invalid and reads the error instead of the hint", () => {
    render(<TextField label="Name" hint="As on your ID" error="Enter your name." />);
    const input = screen.getByLabelText("Name");
    expect(input).toHaveAttribute("aria-invalid", "true");
    expect(input).toHaveAccessibleDescription("Enter your name.");
    expect(screen.queryByText("As on your ID")).toBeNull();
  });

  it("passes input props through", () => {
    const onChange = vi.fn();
    render(<TextField label="Phone" type="tel" maxLength={5} onChange={onChange} />);
    const input = screen.getByLabelText("Phone");
    expect(input).toHaveAttribute("type", "tel");
    expect(input).toHaveAttribute("maxlength", "5");
    fireEvent.change(input, { target: { value: "123" } });
    expect(onChange).toHaveBeenCalled();
  });
});

describe("SelectField", () => {
  it("is a labelled native select that can show an error", () => {
    render(
      <SelectField label="Vehicle type" value="" onChange={() => {}} error="Choose Car or Bike.">
        <option value="">Choose…</option>
        <option value="CAR">Car</option>
      </SelectField>,
    );
    const select = screen.getByLabelText("Vehicle type");
    expect(select.tagName).toBe("SELECT");
    expect(select).toHaveAttribute("aria-invalid", "true");
    expect(select).toHaveAccessibleDescription("Choose Car or Bike.");
  });
});

describe("FormSheet", () => {
  it("renders nothing when closed", () => {
    render(
      <FormSheet open={false} title="Your profile" onClose={() => {}}>
        <p>body</p>
      </FormSheet>,
    );
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("shows a titled dialog that closes with the button and with Escape", () => {
    const onClose = vi.fn();
    render(
      <FormSheet open title="Your profile" onClose={onClose}>
        <p>body</p>
      </FormSheet>,
    );
    expect(screen.getByRole("dialog", { name: "Your profile" })).toHaveTextContent("body");
    fireEvent.click(screen.getByRole("button", { name: "Close your profile" }));
    fireEvent.keyDown(document, { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(2);
  });
});
