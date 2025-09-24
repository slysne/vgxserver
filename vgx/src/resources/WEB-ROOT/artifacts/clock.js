class Clock {

    constructor() {
        this.refresher = null;
    }


    setElement(elem) {
        this.elem = elem;
    }


    running() {
        return this.refresher != null;
    }


    start(reference) {
        if (this.refresher != null) {
            clearInterval(this.refresher);
        }
        this.updateTimeReference(reference);
        this.refresher = setInterval( this.refresh.bind(this), 77 );
    }


    stop() {
        if (this.refresher != null) {
            clearInterval(this.refresher);
            this.refresher = null;
        }
        if (this.elem != null) {
            this.elem.val(this.secondsToDHMS(null));
        }
    }


    refresh() {
        try {
            if (this.elem != null) {
                const seconds = this.getTime();
                const dhms = this.secondsToDHMS(seconds);
                this.elem.val(dhms);
            }
        }
        catch (error) {
        }
    }


    updateTimeReference(reference) {
        const d = new Date();
        this.tms0 = d.getTime();
        this.reference = reference;
    }


    getTime() {
        const d = new Date();
        const tms1 = d.getTime();
        const seconds = this.reference + (tms1 - this.tms0)/1000.0;
        return seconds;
    }


    secondsToDHMS(seconds) {
        if (seconds != null && seconds > 0) {
            const recent_start = seconds < 60;
            const D = Math.floor( seconds / 86400 );
            seconds = seconds % 86400;
            const H = Math.floor( seconds / 3600 );
            seconds = seconds % 3600;
            const M = Math.floor( seconds / 60 );
            const S = Math.floor(seconds % 60);
            let DHMS;
            if (recent_start) {
                const d = (seconds < 10) ? 2 : 1;
                DHMS = seconds.toFixed(d).padStart(2, "0");
            }
            else {
                DHMS = D.toString().padStart(3, "0") + ":" + H.toString().padStart(2, "0") + ":" + M.toString().padStart(2, "0") + ":" + S.toString().padStart(2, "0");
            }
            return DHMS;
        }
        else {
            return "---:--:--:--";
        }
    }
}


