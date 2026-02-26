import pandas as pd

def main():
    # The user shared a link with 'USP=sharing' which means anyone with the link can view.
    # We can use the pandas export trick: change /edit?usp=sharing to /export?format=csv
    # Link: https://docs.google.com/spreadsheets/d/1esOzR5cl1NboGfEBxZSHjG76_4DhHXXCGWLLJMsMRn8/edit?usp=sharing
    
    sheet_id = "1esOzR5cl1NboGfEBxZSHjG76_4DhHXXCGWLLJMsMRn8"
    
    # Let's download our data directly from our output sheet (we know permissions work for it)
    our_url = f"https://docs.google.com/spreadsheets/d/1g5S7UkoNh2lLuwfUr-ssNto4gRZQDnqICqS2rMjdliA/export?format=csv&gid=0"
    
    # Try fetching public CSV
    ref_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
    
    try:
        df_ours = pd.read_csv(our_url)
        print("Our Top 5:")
        print(df_ours.head(5))
    except Exception as e:
        print(f"Failed to fetch our data via CSV: {e}")
        
    try:
        df_ref = pd.read_csv(ref_url)
        print("\nReference Top 5:")
        print(df_ref.head(5))
        
        print("\nReference Columns:")
        print(df_ref.columns.tolist())
    except Exception as e:
        print(f"Failed to fetch reference data via CSV: {e}")

if __name__ == "__main__":
    main()
