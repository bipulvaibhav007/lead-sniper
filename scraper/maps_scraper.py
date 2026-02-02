from playwright.sync_api import sync_playwright
import time
import random

def scrape_google_maps(query, max_results=10, headless=True, progress_callback=None):
    results = []
    
    with sync_playwright() as p:
        # Launch browser (Visible mode)
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={'width': 1280, 'height': 720},
            locale="en-US", # Force English to ensure selectors match
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        
        try:
            print(f"--- Opening Google Maps for: {query} ---")
            page.goto("https://www.google.com/maps?hl=en", timeout=60000)
            
            # 1. Wait for page to settle
            try:
                page.wait_for_load_state("networkidle", timeout=5000)
            except:
                pass # Continue even if network is busy

            # 2. Handle "Before you continue" Cookie Popup
            try:
                # Look for common "Accept" buttons
                for btn_text in ["Accept all", "Agree", "I agree"]:
                    btn = page.get_by_role("button", name=btn_text)
                    if btn.is_visible():
                        print(f"Found cookie button ({btn_text}). Clicking...")
                        btn.click()
                        time.sleep(2)
                        break
            except Exception as e:
                print("Cookie check skipped:", e)

            # 3. Find Search Box (Robust Method)
            print("Looking for search box...")
            search_box = None
            
            # Try specific ID first
            if page.locator("input#searchboxinput").is_visible():
                search_box = page.locator("input#searchboxinput")
            # Try by aria-label
            elif page.locator('input[aria-label="Search Google Maps"]').is_visible():
                search_box = page.locator('input[aria-label="Search Google Maps"]')
            # Try generic input
            elif page.locator('input[name="q"]').is_visible():
                search_box = page.locator('input[name="q"]')
                
            if not search_box:
                print("CRITICAL: Could not find search box. Saving screenshot.")
                page.screenshot(path="debug_no_searchbox.png")
                raise Exception("Search box not found on page.")

            # 4. Type and Search
            search_box.click() # Focus
            time.sleep(0.5)
            search_box.fill(query)
            time.sleep(0.5)
            page.keyboard.press("Enter")
            print("Search query submitted.")

            # 5. Wait for Results
            print("Waiting for results list...")
            # Wait for the left panel to appear
            try:
                page.wait_for_selector('div[role="feed"]', timeout=15000)
            except:
                # Retry: sometimes the layout is different
                print("Standard feed not found, looking for alternative...")
                page.wait_for_selector('div.Nv2PK', timeout=15000)

            # 6. Scroll and Scrape
            feed = page.locator('div[role="feed"]')
            
            # Scroll loop
            while True:
                listings = page.locator('div.Nv2PK')
                count = listings.count()
                
                if count >= max_results:
                    break
                
                # Scroll the feed
                feed.evaluate("el => el.scrollBy(0, el.scrollHeight)")
                time.sleep(2) # Give time for gray boxes to load content
                
                # Check if end of list
                if page.locator("text=You've reached the end of the list").is_visible():
                    break
                
                # Safety break if no new items load for a long time
                if count == listings.count() and count > 0:
                    # try forcing a small scroll up and down
                    feed.evaluate("el => el.scrollBy(0, -100)")
                    time.sleep(0.5)
                    feed.evaluate("el => el.scrollBy(0, 500)")
                    time.sleep(2)
                    if count == listings.count():
                        break

            # 7. Extract Data
            listings = page.locator('div.Nv2PK')
            final_count = min(listings.count(), max_results)
            print(f"Processing {final_count} results...")

            for i in range(final_count):
                if progress_callback:
                    progress_callback(int((i / final_count) * 100))
                
                try:
                    # Click to load details
                    listings.nth(i).click()
                    time.sleep(1) 

                    details = {'name': "N/A", 'phone': "N/A", 'website': "N/A", 'address': "N/A", 'rating': "N/A"}
                    
                    # Name (Try h1)
                    if page.locator("h1.DUwDvf").count() > 0:
                        details['name'] = page.locator("h1.DUwDvf").first.inner_text()
                    
                    # Rating
                    if page.locator("div.F7nice span span").count() > 0:
                        details['rating'] = page.locator("div.F7nice span span").first.inner_text()

                    # Address & Phone (via Aria Labels)
                    # We look for buttons that start with "Address:" or "Phone:"
                    # The localized version might be different, so we check generic data-item-ids too
                    
                    if page.locator('button[data-item-id*="address"]').count() > 0:
                        txt = page.locator('button[data-item-id*="address"]').get_attribute("aria-label") or ""
                        details['address'] = txt.replace("Address: ", "").strip()
                        
                    if page.locator('button[data-item-id*="phone"]').count() > 0:
                        txt = page.locator('button[data-item-id*="phone"]').get_attribute("aria-label") or ""
                        details['phone'] = txt.replace("Phone: ", "").strip()
                        
                    if page.locator('a[data-item-id*="authority"]').count() > 0:
                        details['website'] = page.locator('a[data-item-id*="authority"]').get_attribute("href") or "N/A"

                    print(f"Found: {details['name']}")
                    results.append(details)

                except Exception as e:
                    print(f"Error on item {i}: {e}")

        except Exception as e:
            print(f"Critical Error: {e}")
            page.screenshot(path="error_final.png")
        finally:
            browser.close()
            
    return results