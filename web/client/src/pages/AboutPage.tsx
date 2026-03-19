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
          <p>
            Welcome to Census Visualizer! This is a fully open-source website
            that presents vital demographic statistics and derived metrics about
            United States locations (e.g., states, counties, ZIP Codes,
            neighborhoods.)
          </p>
          <p>
            Definitions/math will be added here soon. Please check the README
            for technical info.
          </p>
          <p>
            <Link to={"/privacy"}>Click here to read the Privacy Policy.</Link>
          </p>

          <p>&copy; 2026 Darren R. Skidmore</p>
        </div>
      </main>
    </div>
  );
}
