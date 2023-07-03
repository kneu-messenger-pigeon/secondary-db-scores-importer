ENV STORAGE_FILE /storage/storage.txt
RUN mkdir /storage && touch /storage/storage.txt && chmod 777 -R /storage/storage.txt
