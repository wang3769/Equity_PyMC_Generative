from __future__ import annotations
from pathlib import Path

def md_to_html(md_text: str) -> str:
    """
    Minimal markdown -> HTML renderer without extra deps.
    Supports: headings (#, ##, ###), paragraphs, code fences, bullet lists.
    This is intentionally simple for a display-only internal dashboard.
    """
    lines = md_text.splitlines()
    out = []
    in_code = False
    in_list = False

    def esc(s: str) -> str:
        return (s.replace("&", "&amp;")
                 .replace("<", "&lt;")
                 .replace(">", "&gt;"))

    for raw in lines:
        line = raw.rstrip("\n")

        if line.strip().startswith("```"):
            if not in_code:
                out.append("<pre><code>")
                in_code = True
            else:
                out.append("</code></pre>")
                in_code = False
            continue

        if in_code:
            out.append(esc(line))
            continue

        if line.startswith("### "):
            if in_list:
                out.append("</ul>"); in_list = False
            out.append(f"<h3>{esc(line[4:])}</h3>")
        elif line.startswith("## "):
            if in_list:
                out.append("</ul>"); in_list = False
            out.append(f"<h2>{esc(line[3:])}</h2>")
        elif line.startswith("# "):
            if in_list:
                out.append("</ul>"); in_list = False
            out.append(f"<h1>{esc(line[2:])}</h1>")
        elif line.strip().startswith("- "):
            if not in_list:
                out.append("<ul>")
                in_list = True
            out.append(f"<li>{esc(line.strip()[2:])}</li>")
        elif line.strip() == "":
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append("<div style='height:8px'></div>")
        else:
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<p>{esc(line)}</p>")

    if in_list:
        out.append("</ul>")
    if in_code:
        out.append("</code></pre>")

    return "\n".join(out)

def load_md_as_html(path: Path) -> str:
    if not path.exists():
        return "<p><em>model_card.md not found.</em></p>"
    return md_to_html(path.read_text(encoding="utf-8"))
