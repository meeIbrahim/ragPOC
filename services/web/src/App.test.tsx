import { render, screen } from "@testing-library/react";
import { expect, test } from "vitest";
import App from "./App";

test("the page names the product Pucho", () => {
  render(<App />);
  expect(screen.getByRole("heading", { level: 1, name: "Pucho" })).toBeInTheDocument();
});
