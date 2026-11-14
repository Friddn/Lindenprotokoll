"""Deterministic screenshot curve digitizer.

Outputs estimated geometric candidates, not original sensor samples. Corners and
plateau boundaries are retained. If a cadence can be inferred from corner spacing,
additional plateau positions are marked as inferred rather than confirmed.
"""
from PIL import Image
from datetime import datetime, timedelta
from collections import Counter
import math


def _curve_score(r, g, b, curve):
    """Return line-colour chroma strength; translucent area fills score lower."""
    if curve == "green":
        return max(0.0, min(g - r * 1.15, g - b * 1.05)) if g > 75 else 0.0
    return max(0.0, min(r - g * 1.15, r - b * 1.05)) if r > 110 else 0.0


def _matches(r, g, b, curve):
    return _curve_score(r, g, b, curve) >= 45


def _trace_samples(px, xl, xr, yt, yb, curve):
    """Trace the saturated curve stroke, not the translucent fill below it.

    For each x column, retain pixels close to that column's strongest curve-colour
    chroma. This avoids the old median-of-all-coloured-pixels failure on charts
    that shade the area beneath the red thermal curve.
    """
    samples = []
    for x in range(xl, xr):
        scored = [(y, _curve_score(*px[x, y], curve)) for y in range(yt, yb)]
        peak = max((score for _, score in scored), default=0.0)
        if peak < 45:
            continue
        # Keep the high-chroma stroke and anti-aliased edge, excluding pale fill.
        ys = [y for y, score in scored if score >= max(45.0, peak * 0.90)]
        if ys:
            samples.append((x, _median(ys)))
    return samples


def _median(values):
    values = sorted(values)
    return values[len(values) // 2]


def digitize(path, date, x_left, x_right, y_top, y_bottom, y_min, y_max, curve):
    if curve not in ("green", "red"):
        raise ValueError("Unbekannte Kurvenfarbe.")
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("Ungültiges Diagramm-Datum.") from exc
    if not all(math.isfinite(float(v)) for v in (x_left,x_right,y_top,y_bottom,y_min,y_max)):
        raise ValueError("Achsenwerte müssen gültige Zahlen sein.")
    if y_max <= y_min:
        raise ValueError("kW-Maximum muss größer als kW-Minimum sein.")
    with Image.open(path) as source:
        im = source.convert("RGB")
    w, h = im.size
    xl=max(0,min(w-1,int(x_left))); xr=max(xl+1,min(w,int(x_right)))
    yt=max(0,min(h-1,int(y_top))); yb=max(yt+1,min(h,int(y_bottom)))
    if xr-xl < 20 or yb-yt < 20:
        raise ValueError("Ungültige Achsenkalibrierung oder zu kleiner Plotbereich.")
    px=im.load()
    samples=_trace_samples(px,xl,xr,yt,yb,curve)
    if len(samples) < 8:
        raise ValueError("Zu wenige Kurvenpixel gefunden. Bitte Plotbereich/Farbe prüfen.")
    # Smooth small anti-aliasing variations without changing broad geometry.
    sm=[]
    for i,(x,y) in enumerate(samples):
        win=[v for _,v in samples[max(0,i-2):min(len(samples),i+3)]]
        sm.append((x,_median(win)))
    # Simplify the raster trace with Ramer-Douglas-Peucker. This suppresses
    # anti-aliasing/stair-step artifacts while retaining meaningful vertices.
    def rdp(points, epsilon=1.5):
        if len(points) <= 2:
            return points
        x1,y1=points[0]; x2,y2=points[-1]
        dx=x2-x1; dy=y2-y1
        denom=math.hypot(dx,dy) or 1.0
        best_dist=-1.0; best_index=0
        for idx,(x,y) in enumerate(points[1:-1],1):
            dist=abs(dy*x-dx*y+x2*y1-y2*x1)/denom
            if dist>best_dist:
                best_dist=dist; best_index=idx
        if best_dist>epsilon:
            left=rdp(points[:best_index+1],epsilon)
            right=rdp(points[best_index:],epsilon)
            return left[:-1]+right
        return [points[0],points[-1]]

    simplified=rdp(sm,1.5)
    corners=[(x,y,"corner",0.68) for x,y in simplified]
    candidates={(x,y,k):(x,y,k,c) for x,y,k,c in corners}
    # Flat line segments are explicit plateau intervals. Preserve both ends;
    # interior equal-valued points are only inferred if cadence is measurable.
    plateaus=[]
    for (xa,ya),(xb,y_next) in zip(simplified,simplified[1:]):
        if xb-xa >= 8 and abs(y_next-ya) <= 1:
            plateau_y=round((ya+y_next)/2)
            plateaus.append((xa,xb,plateau_y))
            candidates[(xa,plateau_y,"plateau-boundary")] = (xa,plateau_y,"plateau-boundary",0.60)
            candidates[(xb,plateau_y,"plateau-boundary")] = (xb,plateau_y,"plateau-boundary",0.60)
    ordered=sorted(candidates.values(), key=lambda p:p[0])
    # Infer a plausible horizontal cadence from recurring corner separations.
    diffs=[b[0]-a[0] for a,b in zip(ordered,ordered[1:]) if 3 <= b[0]-a[0] <= max(4,(xr-xl)//5)]
    cadence_px=None
    if len(diffs) >= 3:
        counts=Counter(round(d) for d in diffs)
        mode,n=counts.most_common(1)[0]
        if n >= 2:
            cadence_px=mode
    if cadence_px:
        for a,b,y in plateaus:
            x=a+cadence_px
            while x < b-cadence_px//2:
                # These are inferred equal-valued samples, never original readings.
                candidates[(x,y,"plateau-cadence-inferred")] = (x,y,"plateau-cadence-inferred",0.35)
                x += cadence_px
    ordered=sorted(candidates.values(), key=lambda p:p[0])
    start=datetime.fromisoformat(date+"T00:00:00")
    result=[]
    for x,y,kind,confidence in ordered:
        minute=(x-xl)/(xr-xl)*1440
        ts=(start+timedelta(minutes=minute)).isoformat(timespec="minutes")
        val=y_max-(y-yt)/(yb-yt)*(y_max-y_min)
        result.append({"ts":ts,"value_kw":round(max(y_min,min(y_max,val)),3),
                       "pixel_x":x,"pixel_y":y,"confidence":confidence,"point_kind":kind})
    # Only report a cadence when a repeated pixel spacing was observed. The
    # distance between isolated corners alone is not evidence of sensor cadence.
    cadence=round(cadence_px*1440/(xr-xl)) if cadence_px else None
    return result,cadence


def resample_15min(points, date):
    """Create explicitly estimated 15-minute values from digitized geometry.

    No extrapolation is performed. Output points are estimates/interpolations,
    not original heat-pump sensor readings.
    """
    from datetime import datetime, timedelta
    start = datetime.fromisoformat(date + "T00:00:00")
    end = start + timedelta(days=1)
    ordered = sorted(
        (datetime.fromisoformat(p["ts"]), float(p["value_kw"]), float(p.get("confidence", 0.0)))
        for p in points
    )
    if len(ordered) < 2:
        return []
    out = []
    j = 0
    for n in range(96):
        target = start + timedelta(minutes=15*n)
        while j + 1 < len(ordered) and ordered[j+1][0] < target:
            j += 1
        if j + 1 >= len(ordered) or ordered[j][0] > target:
            continue
        ta, va, ca = ordered[j]
        tb, vb, cb = ordered[j+1]
        if not (ta <= target <= tb) or tb <= ta:
            continue
        fraction = (target-ta).total_seconds() / (tb-ta).total_seconds()
        value = va + (vb-va)*fraction
        confidence = min(ca, cb) * 0.7
        out.append({"ts": target.isoformat(timespec="minutes"), "value_kw": round(value, 3),
                    "confidence": round(confidence, 3), "point_kind": "estimated-interpolated-15min"})
    return out


def auto_calibrate(path, curve="green"):
    """Automatically locate the chart grid and read its numeric y-axis."""
    import subprocess, shutil, re, tempfile, os
    from PIL import Image
    if curve not in ("green", "red"):
        raise ValueError("Unbekannte Kurvenfarbe.")
    if not shutil.which("tesseract"):
        raise RuntimeError("Tesseract wird für die automatische Achsenerkennung benötigt.")
    with Image.open(path) as src:
        im = src.convert("RGB")
    w, h = im.size
    px = im.load()
    # Restrict grid search to the chart's central content band. Full-width
    # section separators elsewhere on the mobile screen are not plot gridlines.
    # The chart trace itself is normally in the upper-middle portion of this UI.
    x_scan_left, x_scan_right = int(w*.06), int(w*.88)
    y_scan_top, y_scan_bottom = int(h*.25), int(h*.60)
    candidates=[]
    min_span=max(70,int((x_scan_right-x_scan_left)*.10))
    for y in range(y_scan_top, y_scan_bottom):
        xs=[]
        for x in range(x_scan_left, x_scan_right, 2):
            r,g,b=px[x,y]
            if max(r,g,b)-min(r,g,b)<24 and 175<=r<=242:
                xs.append(x)
        if len(xs)>=min_span:
            candidates.append((y,xs))
    bands=[]
    for item in candidates:
        if not bands or item[0]-bands[-1][-1][0]>3:
            bands.append([item])
        else:
            bands[-1].append(item)
    grid=[max(b,key=lambda z:len(z[1])) for b in bands]
    if len(grid)<4:
        raise ValueError("Zu wenige horizontale Gitterlinien im Diagrammbereich erkannt.")
    # Find the longest regularly spaced run; ignore isolated separators and
    # labels that happen to resemble horizontal rules.
    best=[]
    for start_i in range(len(grid)):
        for end_i in range(start_i+3,len(grid)):
            run=grid[start_i:end_i+1]
            gaps=[run[j+1][0]-run[j][0] for j in range(len(run)-1)]
            med=sorted(gaps)[len(gaps)//2]
            if med < 12:
                continue
            if all(abs(g-med)<=max(5,med*.18) for g in gaps) and len(run)>len(best):
                best=run
    if len(best)<4:
        raise ValueError("Diagramm-Gitter ist nicht regelmäßig genug; Achsenskalierung wird nicht geraten.")
    regular=best
    ytop,ybottom=regular[0][0],regular[-1][0]
    _gaps=[b[0]-a[0] for a,b in zip(regular,regular[1:])]
    typical=sorted(_gaps)[len(_gaps)//2]
    lefts=[min(xs) for _,xs in regular]; rights=[max(xs) for _,xs in regular]
    xl=int(sorted(lefts)[len(lefts)//2]); xr=int(sorted(rights)[len(rights)//2])+1
    if xr-xl<w*.35 or ybottom-ytop<h*.08: raise ValueError("Erkannter Diagrammbereich ist zu klein.")
    # Enlarge the y-axis label strip for reliable digit recognition.
    axis=im.crop((xr, max(0,ytop-int(typical*.25)), w, min(h,ybottom+int(typical*.25))))
    axis=axis.resize((axis.width*4,axis.height*4))
    fd,tmp=tempfile.mkstemp(suffix=".png"); os.close(fd)
    try:
        axis.save(tmp)
        proc=subprocess.run(["tesseract",tmp,"stdout","--psm","6",
            "-c","tessedit_char_whitelist=0123456789."],capture_output=True,text=True,timeout=30)
    finally:
        try: os.unlink(tmp)
        except OSError: pass
    nums=[float(v) for v in re.findall(r"(?m)^\s*(\d+(?:\.\d+)?)\s*$",proc.stdout or "")]
    if len(nums)!=len(regular) or len(nums)<2 or any(a<=b for a,b in zip(nums,nums[1:])):
        raise ValueError(f"Y-Achse nicht eindeutig erkannt (Ticks {len(nums)}, Gitterlinien {len(regular)}).")
    trace=[]
    for x in range(xl,xr):
        ys=[y for y in range(ytop,ybottom+1) if _matches(*px[x,y],curve)]
        if ys: trace.append((x,_median(ys)))
    if len(trace)<max(25,int((xr-xl)*.15)):
        raise ValueError("Kurvenfarbe im erkannten Diagrammbereich nicht ausreichend vorhanden.")
    return {"x_left":xl,"x_right":xr,"y_top":ytop,"y_bottom":ybottom,
            "y_min":nums[-1],"y_max":nums[0],"method":"gridline-detection+axis-ocr",
            "tick_count":len(nums)}
