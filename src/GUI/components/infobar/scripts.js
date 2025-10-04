// Update UTC time and date
function updateTime() {
    const now = new Date();
    const hours = String(now.getUTCHours()).padStart(2, '0');
    const minutes = String(now.getUTCMinutes()).padStart(2, '0');
    document.getElementById("time-utc").textContent = `${hours}:${minutes} UTC`;

    const year = now.getUTCFullYear();
    const month = String(now.getUTCMonth() + 1).padStart(2, '0');
    const day = String(now.getUTCDate()).padStart(2, '0');
    document.getElementById("date-display").textContent = `${year}-${month}-${day}`;
}

// Start the clock
updateTime();
setInterval(updateTime, 1000);

// Setup communication with PyQt via WebChannel
new QWebChannel(qt.webChannelTransport, function(channel) {
    window.bridge = channel.objects.bridge;

    const hamburger = document.querySelector(".hamburger-icon");

    hamburger.addEventListener("click", () => {
        hamburger.classList.toggle("active"); // optional visual toggle
        bridge.hamburgerClicked();            // signal PyQt
    });
});
