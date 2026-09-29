import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { ToastProvider } from "@grab-mini/ui";
import { App } from "./App";
import "./app.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ToastProvider>
      <App />
    </ToastProvider>
  </StrictMode>,
);
