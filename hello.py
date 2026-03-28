from flask import Flask, render_template_string

app = Flask(__name__)

HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Hello World</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }

    body {
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
      font-family: 'Segoe UI', sans-serif;
      overflow: hidden;
    }

    .stars {
      position: fixed;
      inset: 0;
      pointer-events: none;
    }

    .star {
      position: absolute;
      width: 2px;
      height: 2px;
      background: white;
      border-radius: 50%;
      animation: twinkle var(--d) ease-in-out infinite alternate;
    }

    @keyframes twinkle {
      from { opacity: 0.1; transform: scale(1); }
      to   { opacity: 1;   transform: scale(1.4); }
    }

    .card {
      text-align: center;
      padding: 60px 80px;
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid rgba(255, 255, 255, 0.15);
      border-radius: 24px;
      backdrop-filter: blur(16px);
      box-shadow: 0 8px 60px rgba(0, 0, 0, 0.5);
      animation: fadeUp 0.8s ease both;
    }

    @keyframes fadeUp {
      from { opacity: 0; transform: translateY(30px); }
      to   { opacity: 1; transform: translateY(0); }
    }

    .emoji {
      font-size: 64px;
      display: block;
      margin-bottom: 24px;
      animation: bounce 2s ease-in-out infinite;
    }

    @keyframes bounce {
      0%, 100% { transform: translateY(0); }
      50%       { transform: translateY(-10px); }
    }

    h1 {
      font-size: 52px;
      font-weight: 800;
      background: linear-gradient(90deg, #a78bfa, #60a5fa, #34d399);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
      margin-bottom: 12px;
    }

    p {
      color: rgba(255, 255, 255, 0.55);
      font-size: 16px;
      letter-spacing: 0.05em;
    }

    .badge {
      display: inline-block;
      margin-top: 32px;
      padding: 6px 18px;
      border-radius: 999px;
      background: rgba(167, 139, 250, 0.15);
      border: 1px solid rgba(167, 139, 250, 0.4);
      color: #a78bfa;
      font-size: 13px;
      letter-spacing: 0.08em;
    }
  </style>
</head>
<body>
  <div class="stars" id="stars"></div>
  <div class="card">
    <span class="emoji">👋</span>
    <h1>Hello, World!</h1>
    <p>Welcome to my fancy Cloud Run service</p>
    <div class="badge">✦ Running on Google Cloud Run ✦</div>
  </div>

  <script>
    const container = document.getElementById('stars');
    for (let i = 0; i < 120; i++) {
      const s = document.createElement('div');
      s.className = 'star';
      s.style.cssText = `
        left: ${Math.random() * 100}%;
        top: ${Math.random() * 100}%;
        --d: ${1.5 + Math.random() * 3}s;
        animation-delay: ${Math.random() * 3}s;
        width: ${1 + Math.random() * 2}px;
        height: ${1 + Math.random() * 2}px;
      `;
      container.appendChild(s);
    }
  </script>
</body>
</html>
"""

@app.route("/")
def hello():
    return render_template_string(HTML)

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
