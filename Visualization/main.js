function colores_google(n) {
    var colores_g = ["#3366cc", "#dc3912", "#ff9900", "#109618", "#990099", "#0099c6", "#dd4477", "#66aa00", "#b82e2e", "#316395", "#994499", "#22aa99", "#aaaa11", "#6633cc", "#e67300", "#8b0707", "#651067", "#329262", "#5574a6", "#3b3eac"];
    return colores_g[n % colores_g.length];
}

$(document).ready(function() {
    // INIT
    let circleCounter = 0;
    let $circleContainer = $('#circles');

    // Get the minimum and maximum cluster size
    const minSize = clusters.reduce((min, curr) => curr.websites.length < min ? curr.websites.length : min, Number.MAX_VALUE);
    const maxSize = clusters.reduce((max, curr) => curr.websites.length > max ? curr.websites.length : max, Number.MIN_VALUE);
    const minOut = 1;
    const maxOut = 5;
    const sizeMap = size => (size - minSize) * (maxOut - minOut) / (maxSize - minSize) + minOut;

    // Pack circles
    let clusterRadiusTuples = clusters.map(c => {
        return {
            radius: sizeMap(c.websites.length),
            cluster: c
        }
    });
    let packer = new Packer(clusterRadiusTuples.map(t => t.radius), $circleContainer.width() / $circleContainer.height());
    let dx = packer.w / 2;
    let dy = packer.h / 2;
    let zx = $circleContainer.width() / packer.w;
    let zy = $circleContainer.height() / packer.h;
    let zoom = zx < zy ? zx : zy;

    // Draw circles
    for (let circle of packer.list) {
        let i = clusterRadiusTuples.findIndex(t => t.radius == circle.r);
        let cluster = clusterRadiusTuples[i].cluster;
        clusterRadiusTuples.splice(i, 1);
        drawCircle(circle, cluster.topterms.slice(0, 3), cluster.websites);
    }

    // FUNCTIONS

    function drawCircle(circle, topEntities, websites) {
        // Create HTML for circle
        let template = Handlebars.compile($("#circle-template").html());
        let html = template({
            id: circleCounter,
            entities: topEntities.map(e => { return { entity: e }; })
        });

        // Add to container
        $circleContainer.append(html);
        let $circle = $circleContainer.find(`#circle-${circleCounter}`);

        // Set color, radius and relocate
        // $circle.css('backgroundColor', randomColor({luminosity: 'light'}));
        $circle.css('backgroundColor', colores_google(circleCounter));
        setRadius($circle, circle.r * zoom);
        relocateByCenter($circle, (circle.c.x + dx) * zoom, (circle.c.y + dy) * zoom);

        // Save default css for animations
        let oldCss = $circle.css(['width', 'height', 'left', 'top', 'fontSize']);
        $circle.data('oldCss', oldCss);
        $circle.data('websites', websites);

        // Animations
        $circle.on('mouseover', e => highlightCircle($circle));
        $circle.on('mouseout', e => resetCircle($circle));
        $circle.on('click', e => showModal(websites));

        // Increase circle counter for unique IDs
        circleCounter++;
    }

    function relocateByCenter($el, x, y) {
        $el.css({
            left: x - $el.width() / 2,
            top: y - $el.height() / 2
        });
    }

    function setRadius($el, radius) {
        $el.width(radius * 2);
        $el.height(radius * 2);
    }

    function highlightCircle($el) {
        const diameter = 200;
        const delta = Math.max(diameter - $el.outerWidth(), 0);

        $el.css('zIndex', 10);
        // $el.css('fontSize', '20px');
        $el.animate({
            width: Math.max($el.outerWidth(), diameter),
            height: Math.max($el.outerHeight(), diameter),
            left: $el.position().left - delta / 2,
            top: $el.position().top - delta / 2,
            fontSize: '20px'
        }, {
            queue: false,
            duration: 200,
        });
    }

    function resetCircle($el) {
        let css = $el.data('oldCss');
        $el.css('zIndex', 1);
        $el.animate(css, {
            queue: false,
            duration: 500
        });
    }

    function showModal(websites) {
        // Create HTML for websites
        let template = Handlebars.compile($("#websites-template").html());
        let html = template({
            websites: websites.map(w => { return { url: w }; })
        });

        // Add HTML to modal
        $('#websites-container').html(html);

        $('#websites-modal').modal();
    }
});