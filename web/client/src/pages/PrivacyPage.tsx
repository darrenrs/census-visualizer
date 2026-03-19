import { Link } from "react-router-dom";

export default function PrivacyPage() {
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
          <h1>Privacy Policy</h1>
          <p>
            Census Visualizer (hereafter referred to as "this site") does not
            use cookies, local storage, or session storage for any purposes. The
            selected geography may appear in the page URL hash so links can be
            shared or revisited.
          </p>
          <p>When you use this site, expect requests to be sent to:</p>
          <ul>
            <li>this site&apos;s API and web server</li>
            <li>OpenStreetMap for base map tiles</li>
            <li>
              <code>geotiles.darrenskidmore.com</code>, which serves PMTiles
              assets from Cloudflare R2
            </li>
          </ul>
          <p>
            This site is hosted on a DigitalOcean VPS with the Nginx web server
            and proxied through Cloudflare. Nginx stores server logs for
            security, debugging, and reliability. At this time Nginx logs
            Cloudflare's proxy IP addresses rather than end-user IP addresses.
            Browser user agent strings are recorded but are not used to identify
            individual users.
          </p>
          <p>
            Cloudflare Web Analytics is enabled. It is used to measure aggregate
            site usage and performance. This site operator uses it for
            high-level traffic and performance insights, not to identify
            individual users.
          </p>
          <p>
            Third-party providers such as Cloudflare, Cloudflare R2, and
            OpenStreetMap may process network and request metadata as part of
            delivering content, caching assets, and providing analytics. This
            may include information such as IP-related network metadata, browser
            type, request path, and timing data.
          </p>
          <p>
            This site is intended for general informational use only. It does
            not provide user accounts, payments, comments, direct messaging, or
            other features that require personal profile data.
          </p>
          <p>
            If this Privacy Policy changes, the updated version will be posted
            on this page.
          </p>
          <p>Last updated 18 March 2026.</p>

          <p>&copy; 2026 Darren R. Skidmore</p>
        </div>
      </main>
    </div>
  );
}
