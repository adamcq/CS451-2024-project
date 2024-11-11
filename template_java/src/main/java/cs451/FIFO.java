package cs451;

public class FIFO {

}
/**
Interface 2 FIFO-order uniform reliable broadcast
        Module:
        Name: FIFO broadcast, instance frb.
        Events:
        Request: ⟨f rb, Send |m⟩: Broadcasts a message m to all processes.
        Indication: ⟨f rb, Deliver |p, m⟩: Delivers a message m broadcast by process p.
        Properties:
        FRB1: Validity: If a correct process p broadcasts a message m, then p eventually delivers m.
        FRB2: No duplication: No message is delivered more than once.
        FRB3: No creation: If a process delivers a message m with sender s, then m was previously broadcast
        by process s.
        FRB4: Uniform agreement: If a message m is delivered by some process (whether correct or faulty),
        then m is eventually delivered by every correct process.
        FRB5: FIFO delivery: If some process broadcasts message m1 before it broadcasts message m2, then
        no correct process delivers m2 unless it has already delivered m1.
**/