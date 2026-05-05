import pandas as pd
import glob

files = sorted(glob.glob('downloads/flipkart_styleid_*.xlsx'))
f = files[-1]
with open('output_check.txt', 'w') as out:
    df = pd.read_excel(f)
    out.write(f'File: {f}\n')
    out.write(f'Columns: {list(df.columns)}\n\n')
    img_cols = [c for c in df.columns if c.startswith('image_')]
    vid_cols = [c for c in df.columns if c.startswith('video_')]
    out.write(f'Image columns: {len(img_cols)}\n')
    out.write(f'Video columns: {len(vid_cols)}\n\n')
    for idx in range(len(df)):
        r = df.iloc[idx]
        sid = r['style_id']
        url_val = r['url']
        out.write(f'=== Product {idx+1}: {sid} ===\n')
        out.write(f'URL: {url_val}\n')
        for c in img_cols:
            val = r[c]
            if pd.notna(val) and val:
                out.write(f'  {c}: {val}\n')
        for c in vid_cols:
            val = r[c]
            if pd.notna(val) and val:
                out.write(f'  {c}: {val}\n')
        out.write('\n')
print('Done - check output_check.txt')
