$(function () {
  $("a#produce").on("click", function (e) {
    e.preventDefault();
    $.getJSON("/my_producer", function (data) {
      //do nothing
    });
    return false;
  });

  $("a#consume").on("click", function (e) {
    e.preventDefault();
    $.getJSON("/my_consumer", function (data) {
      //do nothing
    });
    return false;
  });
});

var socket = io();
// socket.on("connect", function () {
//   console.log("socket connected for stream_event!");
//   socket.emit("stream_event", { data: "I'm connected for stream_event!" });
// });

// socket.on("stream_response", function (msg, cb) {
//   console.log("received response");
//   $("#log").append("<br>" + $("<div/>").text(msg.data).html());
//   if (cb) cb();
// });

// socket.on("produce_connect", function () {
//   console.log("socket connected for produce_event");
//   socket.emit("produce_event", { data: "I'm connected for produce_event!" });
// });

// socket.on("produce_response", function (msg, cb) {
//   console.log("received response");
//   $("#log").append("<br>" + $("<div/>").text(msg.data).html());
//   if (cb) cb();
// });
