$(document).ready(function () {
     $(".sortable-list").sortable({
        items: ".sortable-item.added",
        connectWith: '.sortable-list',
        receive: receive_rules
    });
});

var receive_rules = function(event, ui) {
    var list = $(this);
    if (list.children().length > 3 ||
        list.parent().attr("class") == "column changed2_group") {
            $(ui.sender).sortable('cancel');
            return;
    }
    if (list.children().length == 3 && list.children().attr("id") != "added") {
        var title_selector = "#" + list.parent().attr("id");
        document.querySelector(title_selector + " .type").textContent = "CHANGED";
        document.querySelector(title_selector).className = "column changed_group";

    }
    if ($(ui.sender).children().length == 2) {
        var title_selector = "#" + $(ui.sender).parent().attr("id");
        document.querySelector(title_selector + " .type").textContent = "REMOVED";
        document.querySelector(title_selector).className = "column removed_group";
    }