FROM apache/beam_python3.10_sdk:2.57.0

RUN pip install --upgrade pip

RUN pip install \
    tensorflow-transform==1.15.0 \
    apache-beam[gcp]==2.47.0 \
    tensorflow==2.15 \
    tensorflow-metadata==1.15.0 \
    tfx-bsl==1.15.1 \
    pyarrow==10.0.0

