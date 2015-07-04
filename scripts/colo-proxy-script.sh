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

attach_forward_bridge()
{
    if brctl show |grep -q colobr ; then
        colobr=`brctl show | grep colobr | awk '{print $1}' |tail -1`
        if brctl show $colobr |grep -qw $phy_if ; then
            ip link set $colobr up
            brctl addif $colobr $virt_if
            return 0
        fi
        for (( i=0; i<100; i++))
        do
            if [ "$colobr" == "colobr$i" ]; then
                colobr=colobr$((i+1))
                break
            fi
        done
        if [ $i -eq 100 ]; then
            echo "there are to many colobr"
            colobr=
            exit 1
        fi
    else
        colobr=colobr0
    fi

    brctl addbr $colobr
    ip link set dev $colobr up
    brctl addif $colobr $phy_if
    brctl addif $colobr $virt_if
}

detach_forward_bridge()
{

    bridges="`brctl show | grep -v -e 'bridge name' -e ^$'\t' |\
            awk -F'\t' '{print $1}'`"
    for bridge in $bridges
    do
        if brctl show $bridge |grep -qw $virt_if ; then
                colobr=$bridge
        fi
    done

    if [ "X$colobr" == "X" ]; then
        return
    fi
    brctl delif $colobr $virt_if
    has_slave=`brctl show $colobr |wc -l`
    if [ $has_slave -ne 2 ]; then
        return 0
    fi

    brctl delif $colobr $phy_if
    ip link set $colobr down
    brctl delbr $colobr
}

secondary_install()
{
    attach_forward_bridge

    /usr/local/sbin/iptables -t mangle -I PREROUTING -m physdev --physdev-in \
        $virt_if -j SECCOLO --index $index
    /usr/local/sbin/ip6tables -t mangle -I PREROUTING -m physdev --physdev-in \
        $virt_if -j SECCOLO --index $index
}

secondary_uninstall()
{
    detach_forward_bridge

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
