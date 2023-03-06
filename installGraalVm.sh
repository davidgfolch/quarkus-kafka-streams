#!/bin/bash
set -x
#Install GraalVm https://github.com/graalvm/homebrew-tap
brew install --cask  graalvm/tap/graalvm-ce-java11

#Install GraalVm native
folder=/Library/Java/JavaVirtualMachines/graalvm-ce-java11-22.3.1
sudo xattr -r -d com.apple.quarantine $folder
$folder/Contents/Home/bin/gu install native-image

export GRAALVM_HOME=/Library/Java/JavaVirtualMachines/graalvm-ce-java11-22.3.1/Contents/Home
