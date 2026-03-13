import { Link } from "react-router-dom";

export default function AboutPage() {
  return (
    <div className="app-shell">
      <header className="top-bar">
        <div className="brand">Census Visualizer</div>
        <nav className="top-nav" aria-label="Primary">
          <Link to={"/"}>Home</Link>
          <a
            href="https://github.com/darrenrs/census-visualizer"
            target="_blank"
            rel="noreferrer"
          >
            GitHub
          </a>
        </nav>
      </header>

      <main className="content-shell">
        <div className="text-page">
          <h1>Census Visualizer</h1>
          <p>Future About/FAQ/Privacy page.</p>

          <p>&copy; 2026 Darren R. Skidmore. All rights reserved.</p>
        </div>
      </main>
    </div>
  );
}
