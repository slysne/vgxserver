class UptimeRefresher {

    constructor(destElemId, onResyncCallback, onErrorCallback) {
        this.destElemId = destElemId;
        this.onResyncCallback = onResyncCallback;
        this.onErrorCallback = onErrorCallback;
        this.uptimeCounter = 0;
        this.localEpochAtServerT0 = 0;
        this.serverUptimeSeconds = 0;
        this.serverUpDays = 0;
        this.serverUpHours = 0;
        this.serverUpMinutes = 0;
        this.serverUpSeconds = 0;
        this.uptimeRefresher = null;
        this.uptimeResyncer = null;
    }

    start() {
        UptimeRefresher.resyncServerUptime( this );
    }

    static setUptimeAsync(obj, resyncInterval, localRefreshInterval) {
        if (obj.uptimeRefresher != null) {
            clearInterval(obj.uptimeRefresher);
            obj.uptimeRefresher = null;
        }
        if (obj.uptimeResyncer != null) {
            clearTimeout(obj.uptimeResyncer);
            obj.uptimeResyncer = null;
        }
        if (localRefreshInterval != null) {
            obj.uptimeRefresher = setInterval(UptimeRefresher.refreshUptime, localRefreshInterval, obj);
        }
        obj.uptimeResyncer = setTimeout(UptimeRefresher.resyncServerUptime, resyncInterval, obj);
    }

    getUptimeString() {
        return this.serverUpDays.toString().padStart(3, "0") +
         ":" + this.serverUpHours.toString().padStart(2, "0") +
         ":" + this.serverUpMinutes.toString().padStart(2, "0") +
         ":" + this.serverUpSeconds.toString().padStart(2, "0");
    }

    static refreshUptime( obj ) {
        const t1 = Date.now() / 1000;
        const uptime = Math.round(t1 - obj.localEpochAtServerT0);
        if (uptime > obj.uptimeCounter) {
            obj.uptimeCounter += 1;
            obj.serverUpSeconds += 1;
            if (obj.serverUpSeconds >= 60) {
                obj.serverUpSeconds = 0;
                obj.serverUpMinutes += 1;
                if (obj.serverUpMinutes >= 60) {
                    obj.serverUpMinutes = 0;
                    obj.serverUpHours += 1;
                    if (obj.serverUpHours >= 24) {
                        obj.serverUpHours = 0;
                        obj.serverUpDays += 1;
                    }
                }
            }
            let s = obj.getUptimeString();
            let e = document.getElementById( obj.destElemId );
            if (e.tagName.toLowerCase() == "input") {
                e.value = s;
            }
            else {
                e.innerHTML = s;
            }
        }
    }

    static resyncServerUptime( obj ) {
        $.get("/vgx/time", function (data, textStatus, jqXHR) {
            try {
                const response = data["response"];
                const serverUptime = response["time"]["up"];
                let localEpochSeconds = Date.now() / 1000;
                // parse d:hh:mm:ss into serverUpX variables
                obj.uptimeCounter = obj.serverUptimeSeconds = obj.parseUptime(serverUptime);
                obj.localEpochAtServerT0 = localEpochSeconds - obj.serverUptimeSeconds;
                obj.onResyncCallback();
            }
            catch (error) {
                if (obj.onErrorCallback != null) {
                    obj.onErrorCallback( error );
                }
            }
            finally {
                UptimeRefresher.setUptimeAsync(obj, 10000, 50);
            }
        }).fail(function (xhr, txt, err) {
            UptimeRefresher.setUptimeAsync(obj, 2000, null);
            if (obj.onErrorCallback != null) {
                obj.onErrorCallback( txt );
            }
            let s = "---:--:--:--";
            let e = document.getElementById( obj.destElemId );
            if (e.tagName.toLowerCase() == "input") {
                e.value = s;
            }
            else {
                e.innerHTML = s;
            }
        });

    }

    parseUptime(uptimeStr) {
        let uptime = 0;
        try {
            const rex = /(\d+):(\d+):(\d+):(\d+)/;
            const match = rex.exec(uptimeStr);
            if (match) {
                this.serverUpDays = parseInt(match[1]);
                this.serverUpHours = parseInt(match[2]);
                this.serverUpMinutes = parseInt(match[3]);
                this.serverUpSeconds = parseInt(match[4]);
                uptime = this.serverUpDays * 86400 + this.serverUpHours * 3600 + this.serverUpMinutes * 60 + this.serverUpSeconds;
            }
        }
        catch(error) {
            uptime = -1;
        }
        return uptime;
    }
}



