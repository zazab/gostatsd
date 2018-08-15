package statsdaemon

import (
	"fmt"
	"net"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type MultiConn struct {
	upstreams []net.Conn
}

func MultiDialTimeout(
	proto string,
	addresses []string,
	timeout time.Duration,
) (MultiConn, error) {
	upstreams := []net.Conn{}
	for _, address := range addresses {
		conn, err := net.DialTimeout(proto, address, timeout)
		if err != nil {
			log.Errorf("can't connect to %s: %s", address, err)
			continue
		}

		upstreams = append(upstreams, conn)
	}

	if len(upstreams) != len(addresses) {
		log.Errorf(
			"only %d of %d upstreams connected",
			len(upstreams), len(addresses),
		)
	}

	return MultiConn{upstreams}, nil
}

func (mcon MultiConn) Read(b []byte) (int, error) {
	var (
		retRead int
		errs    = []string{}
	)

	for _, conn := range mcon.upstreams {
		buf := []byte{}
		read, err := conn.Read(buf)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}

		retRead = read
	}

	if len(errs) > 0 {
		return retRead, fmt.Errorf(
			"error reading from upstreams: %s",
			strings.Join(errs, "; "),
		)
	}

	return retRead, nil
}

func (mcon MultiConn) Write(b []byte) (int, error) {
	writenRet := 0
	errs := []string{}
	for _, conn := range mcon.upstreams {
		writen, err := conn.Write(b)
		if err != nil {
			errs = append(errs, err.Error())
		}

		writenRet += writen
	}

	switch len(errs) {
	case len(mcon.upstreams):
		return writenRet, fmt.Errorf(
			"error writing to all upstreams: %s",
			strings.Join(errs, "; "),
		)
	default:
		if len(errs) > 0 {
			log.Errorf(
				"error writing to upstreams: %s",
				strings.Join(errs, "; "),
			)
		}
		return writenRet, nil
	}
}

func (mcon MultiConn) LocalAddr() net.Addr {
	return mcon.upstreams[0].LocalAddr()
}

func (mcon MultiConn) RemoteAddr() net.Addr {
	return mcon.upstreams[0].RemoteAddr()
}

func (mcon MultiConn) SetDeadline(t time.Time) error {

	var errs = make([]string, 0)
	for _, conn := range mcon.upstreams {
		err := conn.SetDeadline(t)
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf(
			"error seting deadline on streams: %s",
			strings.Join(errs, "; "),
		)
	}

	return nil
}

func (mcon MultiConn) SetReadDeadline(t time.Time) error {

	var errs = make([]string, 0)
	for _, conn := range mcon.upstreams {
		err := conn.SetReadDeadline(t)
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf(
			"error setting read deadline on upstreams: %s",
			strings.Join(errs, "; "),
		)
	}

	return nil
}

func (mcon MultiConn) SetWriteDeadline(t time.Time) error {

	var errs = make([]string, 0)
	for _, conn := range mcon.upstreams {
		err := conn.SetWriteDeadline(t)
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf(
			"error setting wriet deadline on upstreams: %s",
			strings.Join(errs, "; "),
		)
	}

	return nil
}

func (mcon MultiConn) Close() error {

	errs := []string{}
	for _, conn := range mcon.upstreams {
		err := conn.Close()
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf(
			"error closing upstreams: %s",
			strings.Join(errs, "; "),
		)
	}

	return nil
}
