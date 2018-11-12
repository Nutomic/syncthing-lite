# Syncthing Lite

[![Build Status](https://travis-ci.org/syncthing/syncthing-lite.svg?branch=master)](https://travis-ci.org/syncthing/syncthing-lite)
[![MPLv2 License](https://img.shields.io/badge/license-MPLv2-blue.svg?style=flat-square)](https://www.mozilla.org/MPL/2.0/)

This project is an Android app, that works as a client for a [Syncthing][1] share (accessing 
Syncthing devices in the same way a client-server file sharing app accesses its proprietary server). 

This is a client-oriented implementation, designed to work online by downloading and 
uploading files from an active device on the network (instead of synchronizing a local copy of 
the entire repository).
Due to that, you will see a sync progress of 0% at other devices (and this is expected).
This is quite different from the way the [syncthing-android][2] works, 
and it's useful for those devices that cannot or do not wish to download the entire repository (for 
example, mobile devices with limited storage available, wishing to access a syncthing share).

This project is based on syncthing-java (which is in this repository too), a java implementation of Syncthing protocols.

Due to the behaviour of this App and the [behaviour of the Syncthing Server](https://github.com/syncthing/syncthing/issues/5224),
you can't reconnect for some minutes if the App was killed (due to removing from the recent App list) or the connection was interrupted.
This does not apply to local discovery connections.

[<img alt="Get it on F-Droid" src="https://f-droid.org/badge/get-it-on.png" height="80">](https://f-droid.org/packages/net.syncthing.lite/)
[<img alt="Get it on Google Play" src="https://play.google.com/intl/en_us/badges/images/generic/en_badge_web_generic.png" height="80">](https://play.google.com/store/apps/details?id=net.syncthing.lite)

## Translations

The project is translated on [Transifex](https://www.transifex.com/syncthing/syncthing-lite/).
Requests for new languages are always accepted (but this happens manually because there is no option to accept it automatically).

## Building

The project uses a standard Android build, and requires the Android SDK. The easiest option is to
install [Android Studio][3] and import the project.

## License
All code is licensed under the [MPLv2 License][4].

[1]: https://syncthing.net/
[2]: https://github.com/syncthing/syncthing-android
[3]: https://developer.android.com/studio/index.html
[4]: LICENSE
