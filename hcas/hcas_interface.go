package hcas

import (
	"os"
)

// Main Higher-archichal content addressable storage (Hcas) interface
//
// Hcas provides an interface for accessing content addressed objects that may
// themselves reference other content addressable objects. This allows tree-like
// data to be stored nicely in a content addressable way.
//
// Hcas uses reference counting to determine when an object can be deleted.
// There are three ways an object can be referenced:
//  1. Another object directly references it
//  2. A label has been associated with it
//  3. An open session is referencing it
//
// If an object has none of the above references it may be garbage collected.
// You cannot directly delete an object in Hcas.
type Hcas interface {
	CreateSession() (Session, error)

	// Open the object as a read-only os.File object. The file will remain
	// readable even if the underlying object is later removed from HCAS.
	ObjectOpen(name []byte) (*os.File, error)

	// Returns a path to the named object. This method does not ensure that the
	// named object actually exists.
	ObjectPath(name []byte) string

	// Close all resources associated with the Hcas instance. All remaining open
	// sessions associated with this Hcas instance will automatically be
	// closed. No method on this or associated session objects may be called
	// again.
	Close() error

	// Collect garbage doing at most 'iterations' units of work. If 'iterations'
	// is <= 0 this will continue until all garbage has been collected.
	GarbageCollect(iterations int) (complete bool, err error)
}

// Represents a session in Hcas. Sessions are used to ensure that objects
// referenced in the session cannot be deleted for the lifetime of
// the session.
type Session interface {
	// Get the object name associated with the passed label. Returns nil if
	// no object is associated with the label.
	//
	// A reference to the returned object will be added into the session's
	// reference list.
	GetLabel(namespace string, label string) ([]byte, error)

	// Set the object associated with the passed label. If name is nil the label
	// will be deleted.
	SetLabel(namespace string, label string, name []byte) error

	// Create a new object with the passed 'data' and the associated dependencies.
	//
	// Returns the name of the created object and adds a reference to it into the
	// session's reference list.
	CreateObject(data []byte, deps ...[]byte) ([]byte, error)

	// Returns an ObjectWriter that allows the caller stream data into a newly
	// created object.
	//
	// After calling Close() the object will be created and a reference will be
	// added to the session's reference list.
	StreamObject(deps ...[]byte) (ObjectWriter, error)

	// Close this session and release any references held to any objects.
	Close() error
}

// Extended io.WriteCloser that allows the client to write into Hcas and access
// the final object name after closing.
type ObjectWriter interface {
	// Standard io.Writer Write() method
	Write(p []byte) (n int, err error)

	// Standard io.Closer Close() method
	Close() error

	// Call Name() after Close() to get the content addressable name of the object
	// written.
	Name() []byte
}
