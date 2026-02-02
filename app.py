import os
import datetime
import threading
import concurrent.futures
from flask import Flask, request, jsonify, render_template, send_file, Response
import pandas as pd

# Import your local scrapers
from scraper.maps_scraper import scrape_google_maps
from scraper.enricher import enrich_business_data

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
RESULT_FOLDER = os.path.join(BASE_DIR, "results")
os.makedirs(RESULT_FOLDER, exist_ok=True)

app = Flask(__name__)
app.secret_key = "marketing_pro_secret"

# Global State
state = {
    "queue": [],
    "current_keyword": "",
    "status": "Idle",
    "progress": 0,
    "total_progress": 0,
    "cancel": False,
    "logs": []
}

def add_log(msg):
    """Adds a timestamped log to the global state."""
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    state["logs"].append(f"[{timestamp}] {msg}")
    # Keep last 100 logs to prevent memory issues
    if len(state["logs"]) > 100:
        state["logs"].pop(0)

# --- ROUTES ---

@app.route("/")
def index():
    """
    Renders the dashboard.
    Gathers file metadata (Name, Size, Date) for the UI cards.
    """
    files_data = []
    # Get all xlsx files
    raw_files = [f for f in os.listdir(RESULT_FOLDER) if f.endswith('.xlsx')]
    # Sort by modification time (Newest first)
    raw_files.sort(key=lambda x: os.path.getmtime(os.path.join(RESULT_FOLDER, x)), reverse=True)
    
    for f in raw_files:
        path = os.path.join(RESULT_FOLDER, f)
        try:
            # Calculate size in KB
            size_kb = round(os.path.getsize(path) / 1024, 1)
            # Format date
            date = datetime.datetime.fromtimestamp(os.path.getmtime(path)).strftime('%Y-%m-%d %H:%M')
            
            files_data.append({
                "name": f,
                "size": f"{size_kb} KB",
                "date": date
            })
        except Exception:
            pass # Skip if file access error

    return render_template("index.html", files=files_data)

@app.route("/status")
def status():
    return jsonify(state)

@app.route("/cancel", methods=["POST"])
def cancel():
    state["cancel"] = True
    add_log("Stopping process by user command...")
    return jsonify({"status": "cancelled"})

@app.route("/download/<filename>")
def download(filename):
    path = os.path.join(RESULT_FOLDER, filename)
    if os.path.exists(path):
        return send_file(path, as_attachment=True)
    return "File not found", 404

# --- NEW: VCard (.vcf) EXPORT FOR MOBILE ---
@app.route("/download_vcf/<filename>")
def download_vcf(filename):
    path = os.path.join(RESULT_FOLDER, filename)
    if not os.path.exists(path): return "File not found", 404
    
    try:
        df = pd.read_excel(path)
        vcf_data = ""
        
        for _, row in df.iterrows():
            name = str(row.get('name', 'Unknown'))
            phone = str(row.get('clean_phone', ''))
            email = str(row.get('best_email', ''))
            org = str(row.get('keyword_source', 'Lead'))
            
            # Skip contacts with no useful info
            if (not phone or phone == 'nan') and (not email or email == 'nan'): 
                continue
            
            vcf_data += "BEGIN:VCARD\nVERSION:3.0\n"
            vcf_data += f"FN:{name}\n"
            vcf_data += f"ORG:{org}\n"
            if phone and phone != 'nan': vcf_data += f"TEL;TYPE=CELL:{phone}\n"
            if email and email != 'nan': vcf_data += f"EMAIL:{email}\n"
            vcf_data += "END:VCARD\n"

        # Return as a downloadable file
        new_filename = filename.replace('.xlsx', '.vcf')
        return Response(
            vcf_data,
            mimetype="text/vcard",
            headers={"Content-disposition": f"attachment; filename={new_filename}"}
        )
    except Exception as e:
        return f"Error creating VCard: {e}", 500

@app.route("/delete/<filename>", methods=["POST"])
def delete_file(filename):
    try:
        path = os.path.join(RESULT_FOLDER, filename)
        if os.path.exists(path):
            os.remove(path)
            return jsonify({"status": "success"})
        else:
            return jsonify({"error": "File does not exist"})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/preview/<filename>")
def preview_file(filename):
    try:
        path = os.path.join(RESULT_FOLDER, filename)
        if not os.path.exists(path): 
            return jsonify({"error": "File not found"})
        
        df = pd.read_excel(path)
        
        # 1. Stats
        total_leads = int(len(df))
        
        # 2. Extract ALL Valid Emails for the Copy Button
        email_list = []
        if 'emails' in df.columns:
            # Get all non-empty email cells
            raw_emails = df['emails'].dropna().astype(str).tolist()
            for row in raw_emails:
                if '@' in row:
                    parts = [e.strip() for e in row.split(',') if '@' in e]
                    email_list.extend(parts)
        
        # Remove duplicates
        email_list = list(set(email_list))
        
        # 3. Generate Table
        preview_html = df.head(5).to_html(classes='preview-table', index=False, border=0)
        
        return jsonify({
            "status": "success",
            "total_leads": total_leads,
            "total_emails": len(email_list),
            "email_list": email_list,
            "html": preview_html
        })
    except Exception as e:
        return jsonify({"error": f"Could not read file: {str(e)}"})

@app.route("/start_bulk", methods=["POST"])
def start_bulk():
    data = request.json
    keywords_raw = data.get("keywords", "")
    max_results = int(data.get("max_results", 10))
    
    # Split keywords by newline and remove empty ones
    keywords = [k.strip() for k in keywords_raw.split("\n") if k.strip()]
    
    if not keywords: 
        return jsonify({"error": "No keywords provided"}), 400

    # Reset State for new campaign
    state["queue"] = keywords
    state["cancel"] = False
    state["logs"] = []
    state["total_progress"] = 0
    state["progress"] = 0
    
    # Run the scraping logic in a background thread
    threading.Thread(target=process_queue, args=(keywords, max_results)).start()
    
    return jsonify({"status": "started"})

# --- MAIN LOGIC ---

def process_queue(keywords, max_results):
    all_data = []
    total_keywords = len(keywords)
    
    add_log(f"Starting Campaign: {len(keywords)} locations/niches.")

    for idx, keyword in enumerate(keywords):
        if state["cancel"]: 
            add_log("Process Cancelled.")
            break
        
        state["current_keyword"] = keyword
        # Update UI text to show which keyword we are on
        state["status"] = f"Scraping ({idx+1}/{total_keywords}): {keyword}"
        add_log(f"Searching: {keyword}")
        
        # 1. SCRAPE MAPS (Headless = True for Silent Mode)
        try:
            raw_leads = scrape_google_maps(
                keyword, 
                max_results=max_results, 
                headless=True,  # SILENT MODE
                progress_callback=lambda p: update_prog(p, 0.5)
            )
        except Exception as e:
            add_log(f"Error scraping maps for '{keyword}': {e}")
            raw_leads = []

        if state["cancel"]: break

        # 2. ENRICH DATA (Find Emails)
        state["status"] = f"Enriching: {keyword}"
        enriched_leads = []
        
        if raw_leads:
            # Use ThreadPool to visit websites concurrently (Faster)
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_lead = {executor.submit(enrich_business_data, lead): lead for lead in raw_leads}
                count = 0
                for future in concurrent.futures.as_completed(future_to_lead):
                    if state["cancel"]: break
                    try:
                        data = future.result()
                        enriched_leads.append(data)
                        count += 1
                        # Update progress bar for the second half (50% -> 100%)
                        update_prog(int((count/len(raw_leads))*100), 0.5, offset=50)
                    except: 
                        pass
        
        # Tag the source keyword
        for lead in enriched_leads:
            lead['keyword_source'] = keyword
            
        all_data.extend(enriched_leads)
        
        # Update Total Batch Progress
        state["total_progress"] = int(((idx + 1) / total_keywords) * 100)
        add_log(f"Finished {keyword}. Found {len(enriched_leads)} leads.")

    # 3. SAVE MASTER REPORT
    if all_data:
        add_log("Generating Excel Report...")
        try:
            df = pd.DataFrame(all_data)
            
            # Organize columns intelligently (ADDED 'icebreaker' HERE)
            cols = [
                'lead_score', 'name', 'best_email', 'icebreaker', 'email_status', 
                'clean_phone', 'keyword_source', 'emails', 'phone', 
                'site_title', 'website', 'address'
            ]
            # Add any extra columns found
            final_cols = [c for c in cols if c in df.columns] + [c for c in df.columns if c not in cols]
            df = df[final_cols]
            
            # Sort by Lead Score (Highest quality first)
            if 'lead_score' in df.columns:
                df = df.sort_values(by='lead_score', ascending=False)
            
            fname = f"Leads_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            df.to_excel(os.path.join(RESULT_FOLDER, fname), index=False)
            add_log(f"SUCCESS: Report saved as {fname}")
        except Exception as e:
            add_log(f"Error saving file: {e}")
    else:
        add_log("Campaign finished but no leads were found.")
    
    state["status"] = "Completed"
    state["progress"] = 100
    state["current_keyword"] = "Done"

def update_prog(val, scale, offset=0):
    """Updates the progress bar percentage for the UI."""
    state["progress"] = int((val * scale) + offset)

if __name__ == "__main__":
    app.run(debug=True, port=5000)