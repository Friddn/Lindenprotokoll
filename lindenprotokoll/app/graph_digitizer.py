"""Deterministic screenshot curve digitizer.

Outputs estimated geometric candidates, not original sensor samples. Corners and
plateau boundaries are retained. If a cadence can be inferred from corner spacing,
additional plateau positions are marked as inferred rather than confirmed.
"""
from PIL import Image
from datetime import datetime, timedelta
from collections import Counter
import math


def _matches(r, g, b, curve):
    if curve == "green":
        return g > 75 and g > r * 1.25 and g > b * 1.12
    return r > 110 and r > g * 1.22 and r > b * 1.12


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
    px=im.load(); samples=[]
    for x in range(xl,xr):
        ys=[y for y in range(yt,yb) if _matches(*px[x,y],curve)]
        if ys:
            samples.append((x,_median(ys)))
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
    for (xa,ya),(xb,yb) in zip(simplified,simplified[1:]):
        if xb-xa >= 8 and abs(yb-ya) <= 1:
            plateaus.append((xa,xb,round((ya+yb)/2)))
            candidates[(xa,round((ya+yb)/2),"plateau-boundary")] = (xa,round((ya+yb)/2),"plateau-boundary",0.60)
            candidates[(xb,round((ya+yb)/2),"plateau-boundary")] = (xb,round((ya+yb)/2),"plateau-boundary",0.60)
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
