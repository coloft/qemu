#!/bin/sh
#usage:
# colo-proxy-script.sh primary/secondary install/uninstall phy_if virt_if index
#.e.g:
# colo-proxy-script.sh primary install eth2 tap0 1

side=$1
action=$2
phy_if=$3
virt_if=$4
index=$5
br=br1
failover_br=br0

script_usage()
{
    echo -n "usage: ./colo-proxy-script.sh primary/secondary "
    echo -e "install/uninstall phy_if virt_if index\n"
}

primary_install()
{
    tc qdisc add dev $virt_if root handle 1: prio
    tc filter add dev $virt_if parent 1: protocol ip prio 10 u32 match u32 \
        0 0 flowid 1:2 action mirred egress mirror dev $phy_if
    tc filter add dev $virt_if parent 1: protocol arp prio 11 u32 match u32 \
        0 0 flowid 1:2 action mirred egress mirror dev $phy_if
    tc filter add dev $virt_if parent 1: protocol ipv6 prio 12 u32 match u32 \
        0 0 flowid 1:2 action mirred egress mirror dev $phy_if

    /usr/local/sbin/iptables -t mangle -I PREROUTING -m physdev --physdev-in \
        $virt_if -j PMYCOLO --index $index --forward-dev $phy_if
    /usr/local/sbin/ip6tables -t mangle -I PREROUTING -m physdev --physdev-in \
        $virt_if -j PMYCOLO --index $index --forward-dev $phy_if
    /usr/local/sbin/arptables -I INPUT -i $phy_if -j MARK --set-mark $index
}

primary_uninstall()
{
    tc filter del dev $virt_if parent 1: protocol ip prio 10 u32 match u32 \
        0 0 flowid 1:2 action mirred egress mirror dev $phy_if
    tc filter del dev $virt_if parent 1: protocol arp prio 11 u32 match u32 \
        0 0 flowid 1:2 action mirred egress mirror dev $phy_if
    tc filter del dev $virt_if parent 1: protocol ipv6 prio 12 u32 match u32 \
        0 0 flowid 1:2 action mirred egress mirror dev $phy_if
    tc qdisc del dev $virt_if root handle 1: prio

    /usr/local/sbin/iptables -t mangle -D PREROUTING -m physdev --physdev-in \
        $virt_if -j PMYCOLO --index $index --forward-dev $phy_if
    /usr/local/sbin/ip6tables -t mangle -D PREROUTING -m physdev --physdev-in \
        $virt_if -j PMYCOLO --index $index --forward-dev $phy_if
    /usr/local/sbin/arptables -D INPUT -i $phy_if -j MARK --set-mark $index
}

secondary_install()
{
    brctl addif $br $phy_if

    /usr/local/sbin/iptables -t mangle -I PREROUTING -m physdev --physdev-in \
        $virt_if -j SECCOLO --index $index
    /usr/local/sbin/ip6tables -t mangle -I PREROUTING -m physdev --physdev-in \
        $virt_if -j SECCOLO --index $index
}

secondary_uninstall()
{
    brctl delif $br $phy_if
    brctl delif $br $virt_if
    brctl addif $failover_br $virt_if

    /usr/local/sbin/iptables -t mangle -D PREROUTING -m physdev --physdev-in \
        $virt_if -j SECCOLO --index $index
    /usr/local/sbin/ip6tables -t mangle -D PREROUTING -m physdev --physdev-in \
        $virt_if -j SECCOLO --index $index
}

if [ $# -ne 5 ]; then
    script_usage
    exit 1
fi

if [ "x$side" != "xprimary" ] && [ "x$side" != "xsecondary" ]; then
    script_usage
    exit 2
fi

if [ "x$action" != "xinstall" ] && [ "x$action" != "xuninstall" ]; then
    script_usage
    exit 3
fi

${side}_${action}
