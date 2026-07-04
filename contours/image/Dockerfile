FROM romainquidet/generate-osm-contours:1.0
COPY patch_nasasrtmutil.py /tmp/patch_nasasrtmutil.py
COPY generate_osm.sh /usr/src/app/generate_osm.sh
RUN python3 /tmp/patch_nasasrtmutil.py
RUN chmod 755 /usr/src/app/generate_osm.sh
RUN python3 -m py_compile /usr/lib/python3/dist-packages/phyghtmap/NASASRTMUtil.py
RUN python3 - <<'__VERIFY__'
import phyghtmap.NASASRTMUtil as n
assert hasattr(n, "_nsv_flat_hgt_for_missing_area")
print("flat404 patch import verification ok")
__VERIFY__
