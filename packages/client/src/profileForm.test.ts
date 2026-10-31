import { describe, expect, it } from "vitest";
import { ApiError } from "./api";
import {
  normalizePlate,
  profileFailure,
  tidy,
  toDriverInput,
  toRiderInput,
  validateDescription,
  validateDriverForm,
  validateName,
  validatePhone,
  validatePlate,
  validateRiderForm,
  vehicleChanged,
  type DriverFormValues,
} from "./profileForm";

const driver = (over: Partial<DriverFormValues> = {}): DriverFormValues => ({
  name: "Pham Van Cuong",
  phone: "",
  vehicleType: "CAR",
  plate: "59-X1 123.45",
  model: "",
  color: "",
  ...over,
});

describe("text helpers", () => {
  it("tidies white space and upper-cases plates", () => {
    expect(tidy("  Nguyen \t Van   An ")).toBe("Nguyen Van An");
    expect(normalizePlate(" 51a-123.45 ")).toBe("51A-123.45");
  });
});

describe("validateName", () => {
  it("accepts names of any script, and requires a letter", () => {
    expect(validateName("Nguyễn Văn An")).toBeNull();
    expect(validateName("山田 太郎")).toBeNull();
    expect(validateName("")).toBe("Enter your name.");
    expect(validateName("   ")).toBe("Enter your name.");
    expect(validateName("12345")).toBe("A name needs at least one letter.");
  });

  it("counts characters, not bytes, and refuses control and bidi characters", () => {
    expect(validateName("a".repeat(80))).toBeNull();
    expect(validateName("a".repeat(81))).toContain("80");
    expect(validateName("é".repeat(80))).toBeNull();
    expect(validateName("An\nBinh")).not.toBeNull();
    expect(validateName(`An${String.fromCharCode(0x202e)}Binh`)).not.toBeNull();
    expect(validateName(`An${String.fromCharCode(0x2028)}Binh`)).not.toBeNull();
  });
});

describe("validatePhone", () => {
  it("is optional, and checks only the shape: 7 to 15 digits", () => {
    expect(validatePhone("")).toBeNull();
    expect(validatePhone("+84 901 234 567")).toBeNull();
    expect(validatePhone("(028) 3822-1234")).toBeNull();
    expect(validatePhone("123456")).toContain("7 to 15");
    expect(validatePhone("1".repeat(16))).toContain("7 to 15");
    expect(validatePhone("0901abc567")).not.toBeNull();
    expect(validatePhone("090+1234567")).not.toBeNull();
  });
});

describe("validatePlate", () => {
  it("accepts Vietnamese and foreign plates, stores no judgement about which is real", () => {
    for (const plate of ["51A-123.45", "59-x1 123.45", "29B1 999.99", "ABC 123", "東京 300 あ 12-34"]) expect(validatePlate(plate)).toBeNull();
  });

  it("requires 3 to 16 characters with at least 3 letters or digits", () => {
    expect(validatePlate("")).toBe("Enter the plate number.");
    expect(validatePlate("A1")).toContain("3 to 16");
    expect(validatePlate("A-.")).toBe("A plate needs at least 3 letters or digits.");
    expect(validatePlate("1".repeat(17))).toContain("3 to 16");
    expect(validatePlate("51A_12345")).not.toBeNull();
    expect(validatePlate("51A\t12345")).not.toBeNull();
  });
});

describe("validateDescription", () => {
  it("is optional text up to a limit", () => {
    expect(validateDescription("", 30)).toBeNull();
    expect(validateDescription("x".repeat(30), 30)).toBeNull();
    expect(validateDescription("x".repeat(31), 30)).toBe("At most 30 characters.");
  });
});

describe("forms", () => {
  it("validates the rider form per field and builds the input the service stores", () => {
    expect(validateRiderForm({ name: "An", phone: "" })).toEqual({});
    expect(validateRiderForm({ name: "", phone: "12" })).toEqual({ name: "Enter your name.", phone: expect.any(String) });
    expect(toRiderInput({ name: "  An   Binh ", phone: " 0901 234 567 " })).toEqual({ name: "An Binh", phone: "0901 234 567" });
  });

  it("requires a vehicle type and plate to apply, but not for a draft", () => {
    expect(validateDriverForm(driver())).toEqual({});
    expect(validateDriverForm(driver({ vehicleType: "", plate: "" }))).toEqual({
      vehicleType: "Choose Car or Bike.",
      plate: "Enter the plate number.",
    });
    expect(validateDriverForm(driver({ vehicleType: "", plate: "" }), false)).toEqual({});
    expect(validateDriverForm(driver({ vehicleType: "", plate: "A" }), false).plate).toBeDefined();
    expect(validateDriverForm(driver({ model: "x".repeat(51), color: "y".repeat(31) }))).toEqual({
      model: "At most 50 characters.",
      color: "At most 30 characters.",
    });
  });

  it("builds the driver input: tidy text, capital plate, no vehicle type until one is chosen", () => {
    expect(toDriverInput(driver({ name: " Cuong ", plate: " 59-x1  123.45", model: " Honda  City " }))).toEqual({
      name: "Cuong",
      phone: "",
      vehicleType: "CAR",
      plate: "59-X1 123.45",
      model: "Honda City",
      color: "",
    });
    expect(toDriverInput(driver({ vehicleType: "" }))).not.toHaveProperty("vehicleType");
  });

  it("detects a change of plate or vehicle type, not of case or spacing", () => {
    const before = { vehicleType: "CAR", plate: "59-X1 123.45" };
    expect(vehicleChanged(before, { vehicleType: "CAR", plate: " 59-x1   123.45 " })).toBe(false);
    expect(vehicleChanged(before, { vehicleType: "BIKE", plate: "59-X1 123.45" })).toBe(true);
    expect(vehicleChanged(before, { vehicleType: "CAR", plate: "51A-123.45" })).toBe(true);
  });
});

describe("profileFailure", () => {
  const err = (status: number, code: string, body: Record<string, unknown> = {}, message = `m${status}`) =>
    new ApiError(message, status, { code, ...body });

  it("names the field for an invalid profile and uses the service's wording", () => {
    expect(profileFailure(err(400, "invalid_profile", { field: "plate" }, "plate is not valid"))).toEqual({ field: "plate", message: "plate is not valid" });
    expect(profileFailure(err(422, "incomplete_profile", { field: "vehicleType" })).field).toBe("vehicleType");
  });

  it("explains refusals that name no field", () => {
    expect(profileFailure(err(403, "forbidden")).field).toBeNull();
    expect(profileFailure(err(409, "suspended")).message).toContain("suspended");
    expect(profileFailure(err(429, "rate_limited")).message).toContain("Too many");
  });

  it("falls back to the error text, or a generic line for something that isn't an error", () => {
    expect(profileFailure(new TypeError("Failed to fetch"))).toEqual({ field: null, message: "Failed to fetch" });
    expect(profileFailure("boom").message).toBe("Couldn't reach the profile service.");
  });
});
