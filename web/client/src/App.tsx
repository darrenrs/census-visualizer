import { Routes, Route, Navigate } from "react-router-dom";
import MainPage from "@/pages/MainPage";
import AboutPage from "@/pages/AboutPage";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<MainPage />} />
      <Route path="/about" element={<AboutPage />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
