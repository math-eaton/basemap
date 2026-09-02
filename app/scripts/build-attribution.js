import { marked } from 'marked';
import { readFileSync, writeFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

// Read the markdown file
const mdContent = readFileSync(
  join(__dirname, '../public/attribution.md'), 
  'utf-8'
);

// Convert to HTML
const htmlContent = marked.parse(mdContent);

const styles = `
* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', 'Fira Sans', 'Droid Sans', 'Helvetica Neue', sans-serif;
    line-height: 1.6;
    color: #333;
    background: #f5f5f5;
    padding: 20px;
}

.container {
    max-width: 1200px;
    margin: 0 auto;
    background: white;
    padding: 40px;
    border-radius: 8px;
    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
}

h1 {
    color: #2c3e50;
    border-bottom: 3px solid #3498db;
    padding-bottom: 10px;
    margin-bottom: 20px;
    font-size: 2.5em;
}

h2 {
    color: #2c3e50;
    margin-top: 40px;
    margin-bottom: 15px;
    font-size: 1.8em;
    border-bottom: 2px solid #ecf0f1;
    padding-bottom: 8px;
}

h3 {
    color: #34495e;
    margin-top: 25px;
    margin-bottom: 12px;
    font-size: 1.3em;
}

h4 {
    color: #34495e;
    margin-top: 20px;
    margin-bottom: 10px;
    font-size: 1.1em;
}

p {
    margin-bottom: 15px;
}

a {
    color: #3498db;
    text-decoration: none;
    transition: color 0.2s;
}

a:hover {
    color: #2980b9;
    text-decoration: underline;
}

ul, ol {
    margin-left: 30px;
    margin-bottom: 15px;
}

li {
    margin-bottom: 8px;
}

code {
    background: #f8f9fa;
    padding: 2px 6px;
    border-radius: 3px;
    font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
    font-size: 0.9em;
    color: #e74c3c;
}

pre {
    background: #2c3e50;
    color: #ecf0f1;
    padding: 15px;
    border-radius: 5px;
    overflow-x: auto;
    margin-bottom: 20px;
    font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
    font-size: 0.9em;
}

pre code {
    background: transparent;
    color: #ecf0f1;
    padding: 0;
}

table {
    width: 100%;
    border-collapse: collapse;
    margin: 20px 0;
    font-size: 0.95em;
}

th, td {
    padding: 12px;
    text-align: left;
    border: 1px solid #ddd;
}

th {
    background: #3498db;
    color: white;
    font-weight: 600;
}

tr:nth-child(even) {
    background: #f8f9fa;
}

tr:hover {
    background: #e8f4f8;
}

.metadata {
    color: #7f8c8d;
    font-style: italic;
    margin-bottom: 30px;
}

.source-card {
    background: #f8f9fa;
    border-left: 4px solid #3498db;
    padding: 20px;
    margin: 25px 0;
    border-radius: 4px;
}

.source-card h2 {
    margin-top: 0;
    border-bottom: none;
}

.attribution-box {
    background: #fff3cd;
    border: 1px solid #ffc107;
    border-radius: 5px;
    padding: 15px;
    margin: 20px 0;
}

.attribution-box strong {
    color: #856404;
}

.license-box {
    background: #d1ecf1;
    border: 1px solid #17a2b8;
    border-radius: 5px;
    padding: 15px;
    margin: 20px 0;
}

.technical-specs {
    background: #e7f3ff;
    padding: 15px;
    border-radius: 5px;
    margin: 20px 0;
}

.badge {
    display: inline-block;
    padding: 4px 8px;
    border-radius: 3px;
    font-size: 0.85em;
    font-weight: 600;
    margin-right: 5px;
    margin-bottom: 5px;
}

.badge-primary {
    background: #3498db;
    color: white;
}

.badge-success {
    background: #27ae60;
    color: white;
}

.badge-info {
    background: #17a2b8;
    color: white;
}

.badge-warning {
    background: #f39c12;
    color: white;
}

hr {
    border: none;
    border-top: 2px solid #ecf0f1;
    margin: 40px 0;
}

.back-link {
    display: inline-block;
    background: #3498db;
    color: white;
    padding: 10px 20px;
    border-radius: 5px;
    text-decoration: none;
    margin-bottom: 20px;
    transition: background 0.2s;
    cursor: pointer;
}

.back-link:hover {
    background: #2980b9;
    text-decoration: none;
}

.toc {
    background: #f8f9fa;
    padding: 20px;
    border-radius: 5px;
    margin: 30px 0;
}

.toc ul {
    list-style: none;
    margin-left: 0;
}

.toc li {
    margin-bottom: 10px;
}

@media (max-width: 768px) {
    body {
        padding: 10px;
    }

    .container {
        padding: 20px;
    }

    h1 {
        font-size: 2em;
    }

    h2 {
        font-size: 1.5em;
    }

    table {
        font-size: 0.85em;
    }

    th, td {
        padding: 8px;
    }
}
`;

// Wrap in full HTML document
const fullHtml = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Attribution - GRID3 Map Tiles</title>
    <style>${styles}</style>
</head>
<body>
    <div class="container">
        <a href="#" class="back-link" onclick="event.preventDefault(); window.history.length > 1 ? window.history.back() : window.location.href = '/';">← Back to Map</a>
        
        ${htmlContent}
        
        <a href="#" class="back-link" id="backButton">← Back to Map</a>
    </div>
    
    <script>
        // Make the back button work intelligently
        document.getElementById('backButton').addEventListener('click', function(e) {
            e.preventDefault();
            
            // If opened in a new tab from the map, this will have history
            if (window.history.length > 1) {
                window.history.back();
            } else {
                // Otherwise go to the root
                window.location.href = '/';
            }
        });
    </script>
</body>
</html>`;

// Write the output
writeFileSync(
  join(__dirname, '../public/attribution.html'),
  fullHtml
);

console.log('✓ Generated attribution.html from Markdown');
