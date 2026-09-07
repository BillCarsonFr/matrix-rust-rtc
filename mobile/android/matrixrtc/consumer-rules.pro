# Applied to any app that consumes the AAR (consumerProguardFiles).

# The generated bindings reach the native library through JNA, which looks up
# its own classes reflectively and marshals callbacks by method name.
-keep class com.sun.jna.** { *; }
-keep class * implements com.sun.jna.** { *; }
-dontwarn java.awt.**

# Generated uniffi structures and callback interfaces are resolved by name too.
-keep class org.matrix.rtc.** { *; }

# libwebrtc (media variant) calls back into its Java classes from native code.
-keep class livekit.org.webrtc.** { *; }
-dontwarn livekit.org.webrtc.**
