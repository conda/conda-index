import time
from xml.dom.minidom import getDOMImplementation


def get_recent_packages(channeldata, threshold_days):

    threshold = time.time() - threshold_days * 24 * 60 * 60

    def all_packages():
        for name, package in channeldata.get("packages", {}).items():
            yield {name: package}
        for name, package in channeldata.get("packages.conda", {}).items():
            yield {name: package}

    def find_recent_packages():
        for package in all_packages():
            if tuple(package.values())[0].get("timestamp", threshold) > threshold:
                yield package

    return sorted(
        find_recent_packages(),
        key=lambda x: tuple(x.values())[0]["timestamp"],
        reverse=True,
    )


def _iso822(timestamp):
    return time.strftime("%a, %d %b %Y %T GMT", time.gmtime(timestamp))


def _get_channel(channel_name, packages, threshold_days):
    return {
        "title": f"anaconda.org/{channel_name}",
        "link": f"https://conda.anaconda.org/{channel_name}",
        "description": f"An anaconda.org community with {len(packages)} package updates in the past {threshold_days} days.",
        "pubDate": _iso822(time.time()),
        "lastBuildDate": _iso822(time.time()),
    }


def _get_title(name, version, subdirs):
    return f"{name} {version} [{', '.join(sorted({x for x in subdirs}))}]"


def _get_items(packages):

    items = []
    for name, package in [tuple(p.items())[0] for p in packages]:
        __ = lambda x: package.get(x)

        def coalesce(*args, default="No description."):
            for arg in [a for a in args if __(a)]:
                return package[arg]
            return default

        item = {
            # Example: "7zip 19.00 [osx-64, win-64]"
            "title": _get_title(name, __("version"), __("subdirs")),
            "description": coalesce("description", "summary"),
            "link": __("doc_url"),  # URI - project or project docs
            "comments": __("dev_url"),  # URI
            "guid": __("source_url"),  # URI - download link
            "pubDate": _iso822(__("timestamp")),
            "source": __("home"),  # URI
        }
        empty_fields = [k for k, v in item.items() if not v]
        for k in empty_fields:
            del item[k]
        items.append(item)
    return items


def get_rss(channel_name, channeldata, threshold_days):
    newdoc = getDOMImplementation().createDocument(None, "rss", None)

    def append_strings(node, strings):
        for key, value in strings.items():
            key = newdoc.createElement(key)
            key.appendChild(newdoc.createTextNode(str(value)))
            node.appendChild(key)

    packages = get_recent_packages(channeldata, threshold_days)

    channel = newdoc.createElement("channel")
    append_strings(channel, _get_channel(channel_name, packages, threshold_days))

    for package in _get_items(packages):
        item = newdoc.createElement("item")
        append_strings(item, package)
        channel.appendChild(item)

    rss = newdoc.documentElement
    rss.setAttribute("version", "2.0")
    rss.appendChild(channel)
    return newdoc.toprettyxml(indent="    ")


if __name__ == "__main__":  # pragma: no cover
    import sys
    import json

    channel, channeldata_fn, threshold_days = sys.argv[1:]
    with open(channeldata_fn) as fd:
        print(get_rss(channel, json.load(fd), int(threshold_days)))
